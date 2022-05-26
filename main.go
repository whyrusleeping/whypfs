package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-cidutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/mitchellh/go-homedir"
	"github.com/multiformats/go-multihash"

	cli "github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"

	echo "github.com/labstack/echo/v4"
)

func main() {
	app := cli.NewApp()

	home, err := homedir.Dir()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  "repo",
			Usage: "specify default node repo location",
			Value: filepath.Join(home, ".whypfs"),
		},
		&cli.StringFlag{
			Name:  "blockstore",
			Usage: "specify alternate blockstore",
		},
		&cli.StringSliceFlag{
			Name:  "listen-addr",
			Usage: "specify libp2p listen multiaddrs",
			Value: cli.NewStringSlice("/ip4/0.0.0.0/tcp/9490"),
		},
		&cli.StringFlag{
			Name:  "api",
			Usage: "specify http api listen address",
			Value: "127.0.0.1:5005",
		},
	}
	app.Action = func(cctx *cli.Context) error {

		ctx := context.TODO()

		repo := cctx.String("repo")

		if err := ensureRepoExists(repo); err != nil {
			return err
		}

		bstore := cctx.String("blockstore")
		if bstore == "" {
			bstore = ":flatfs:" + filepath.Join(repo, "blocks")
		}

		cfg := &Config{
			Libp2pKeyFile:     filepath.Join(repo, "libp2p.key"),
			ListenAddrs:       cctx.StringSlice("listen-addr"),
			AnnounceAddrs:     nil,
			DatastoreDir:      filepath.Join(repo, "datastore"),
			Blockstore:        bstore,
			NoBlockstoreCache: false,
			NoLimiter:         true,
			BitswapConfig: BitswapConfig{
				MaxOutstandingBytesPerPeer: 20 << 20,
				TargetMessageSize:          2 << 20,
			},
			//LimitsConfig            Limits
			ConnectionManagerConfig: ConnectionManager{},
			DatabaseConnString:      "sqlite=whypfs.db",
		}

		nd, err := Setup(ctx, cfg)
		if err != nil {
			return err
		}

		nd.tracer = otel.Tracer("node")

		s := &Server{
			Node:   nd,
			tracer: otel.Tracer("api"),
		}

		e := echo.New()

		unixfs := e.Group("/unixfs")
		unixfs.POST("/add", s.HandleAddFile)

		repo := e.Group("/repo")

		return e.Start(cctx.String("api"))
	}

	app.RunAndExitOnError()
}

type Server struct {
	tracer trace.Tracer

	Node *Node
}

func (s *Server) HandleAddFile(c echo.Context) error {
	ctx, span := s.tracer.Start(c.Request().Context(), "handleAddFile")
	defer span.End()

	form, err := c.MultipartForm()
	if err != nil {
		return err
	}

	defer form.RemoveAll()

	mpf, err := c.FormFile("data")
	if err != nil {
		return err
	}

	fname := mpf.Filename
	if fvname := c.FormValue("name"); fvname != "" {
		fname = fvname
	}

	fi, err := mpf.Open()
	if err != nil {
		return err
	}

	defer fi.Close()

	bserv := blockservice.New(s.Node.Blockstore, nil)
	dserv := merkledag.NewDAGService(bserv)

	nd, err := s.importFile(ctx, dserv, fi)
	if err != nil {
		return err
	}

	/*
		if c.QueryParam("ignore-dupes") == "true" {
			isDup, err := s.isDupCIDContent(c, nd.Cid(), u)
			if err != nil || isDup {
				return err
			}
		}
	*/

	content, err := s.Node.addDatabaseTracking(ctx, dserv, s.Node.Blockstore, nd.Cid(), fname)
	if err != nil {
		return xerrors.Errorf("encountered problem computing object references: %w", err)
	}

	if c.QueryParam("lazy-provide") != "true" {
		subctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()
		if err := s.Node.FullRT.Provide(subctx, nd.Cid(), true); err != nil {
			span.RecordError(fmt.Errorf("provide error: %w", err))
			log.Errorf("fullrt provide call errored: %s", err)
		}
	}

	go func() {
		if err := s.Node.Provider.Provide(nd.Cid()); err != nil {
			log.Warnf("failed to announce providers: %s", err)
		}
	}()

	return c.JSON(200, &FileAddResponse{
		Cid:    nd.Cid().String(),
		FileID: content.ID,
	})
}

type FileAddResponse struct {
	Cid    string `json:"cid"`
	FileID uint   `json:"fileID"`
}

func (nd *Node) addDatabaseTracking(ctx context.Context, dserv ipld.NodeGetter, bs blockstore.Blockstore, root cid.Cid, fname string) (*Pin, error) {
	ctx, span := nd.tracer.Start(ctx, "computeObjRefs")
	defer span.End()

	pin := &Pin{
		Cid:     DbCID{root},
		Name:    fname,
		Active:  false,
		Pinning: true,
	}

	if err := nd.DB.Create(pin).Error; err != nil {
		return nil, xerrors.Errorf("failed to track new pin in database: %w", err)
	}

	if err := nd.addDatabaseTrackingToPin(ctx, pin.ID, dserv, bs, root, func(int64) {}); err != nil {
		return nil, err
	}

	return pin, nil
}

const noDataTimeout = time.Minute

func (nd *Node) addDatabaseTrackingToPin(ctx context.Context, pin uint, dserv ipld.NodeGetter, bs blockstore.Blockstore, root cid.Cid, cb func(int64)) error {
	ctx, span := nd.tracer.Start(ctx, "computeObjRefsUpdate")
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	gotData := make(chan struct{}, 1)
	go func() {
		nodata := time.NewTimer(noDataTimeout)
		defer nodata.Stop()

		for {
			select {
			case <-nodata.C:
				cancel()
			case <-gotData:
				nodata.Reset(noDataTimeout)
			case <-ctx.Done():
				return
			}
		}
	}()

	var objlk sync.Mutex
	var objects []*Object
	cset := cid.NewSet()

	defer func() {
		nd.inflightCidsLk.Lock()
		_ = cset.ForEach(func(c cid.Cid) error {
			v, ok := nd.inflightCids[c]
			if !ok || v <= 0 {
				log.Errorf("cid should be inflight but isn't: %s", c)
			}

			nd.inflightCids[c]--
			if nd.inflightCids[c] == 0 {
				delete(nd.inflightCids, c)
			}
			return nil
		})
		nd.inflightCidsLk.Unlock()
	}()

	err := merkledag.Walk(ctx, func(ctx context.Context, c cid.Cid) ([]*ipld.Link, error) {
		// cset.Visit gets called first, so if we reach here we should immediately track the CID
		nd.inflightCidsLk.Lock()
		nd.inflightCids[c]++
		nd.inflightCidsLk.Unlock()

		node, err := dserv.Get(ctx, c)
		if err != nil {
			return nil, err
		}

		cb(int64(len(node.RawData())))

		select {
		case gotData <- struct{}{}:
		case <-ctx.Done():
		}

		objlk.Lock()
		objects = append(objects, &Object{
			Cid:  DbCID{c},
			Size: len(node.RawData()),
		})
		objlk.Unlock()

		if c.Type() == cid.Raw {
			return nil, nil
		}

		return node.Links(), nil
	}, root, cset.Visit, merkledag.Concurrent())

	if err != nil {
		return err
	}

	if err = nd.addObjectsToDatabase(ctx, pin, dserv, root, objects); err != nil {
		return err
	}

	return nil
}

// addObjectsToDatabase creates entries on the estuary database for CIDs related to an already pinned CID (`root`)
// These entries are saved on the `objects` table, while metadata about the `root` CID is mostly kept on the `contents` table
// The link between the `objects` and `contents` tables is the `obj_refs` table
func (nd *Node) addObjectsToDatabase(ctx context.Context, content uint, dserv ipld.NodeGetter, root cid.Cid, objects []*Object) error {
	ctx, span := nd.tracer.Start(ctx, "addObjectsToDatabase")
	defer span.End()

	if err := nd.DB.CreateInBatches(objects, 300).Error; err != nil {
		return xerrors.Errorf("failed to create objects in db: %w", err)
	}

	refs := make([]ObjRef, 0, len(objects))
	var totalSize int64
	for _, o := range objects {
		refs = append(refs, ObjRef{
			Content: content,
			Object:  o.ID,
		})
		totalSize += int64(o.Size)
	}

	span.SetAttributes(
		attribute.Int64("totalSize", totalSize),
		attribute.Int("numObjects", len(objects)),
	)

	if err := nd.DB.Model(Pin{}).Where("id = ?", content).UpdateColumns(map[string]interface{}{
		"active":  true,
		"size":    totalSize,
		"pinning": false,
	}).Error; err != nil {
		return xerrors.Errorf("failed to update content in database: %w", err)
	}

	if err := nd.DB.CreateInBatches(refs, 500).Error; err != nil {
		return xerrors.Errorf("failed to create refs: %w", err)
	}

	return nil
}

func (s *Server) importFile(ctx context.Context, dserv ipld.DAGService, fi io.Reader) (ipld.Node, error) {
	_, span := s.tracer.Start(ctx, "importFile")
	defer span.End()

	return ImportFile(dserv, fi)
}

const DefaultHashFunction = uint64(multihash.SHA2_256)

func ImportFile(dserv ipld.DAGService, fi io.Reader) (ipld.Node, error) {
	prefix, err := merkledag.PrefixForCidVersion(1)
	if err != nil {
		return nil, err
	}
	prefix.MhType = DefaultHashFunction

	spl := chunker.NewSizeSplitter(fi, 1024*1024)
	dbp := ihelper.DagBuilderParams{
		Maxlinks:  1024,
		RawLeaves: true,

		CidBuilder: cidutil.InlineBuilder{
			Builder: prefix,
			Limit:   32,
		},

		Dagserv: dserv,
	}

	db, err := dbp.New(spl)
	if err != nil {
		return nil, err
	}

	return balanced.Layout(db)
}

func ensureRepoExists(dir string) error {
	st, err := os.Stat(dir)
	if err == nil {
		if st.IsDir() {
			return nil
		}
		return fmt.Errorf("repo dir was not a directory")
	}

	if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	return nil
}
