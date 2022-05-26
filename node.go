package main

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-bitswap"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	flatfs "github.com/ipfs/go-ds-flatfs"
	levelds "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-ipfs-provider/batched"
	"github.com/ipfs/go-ipfs-provider/queue"
	metri "github.com/ipfs/go-metrics-interface"
	"github.com/libp2p/go-libp2p"
	connmgr "github.com/libp2p/go-libp2p-connmgr"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	metrics "github.com/libp2p/go-libp2p-core/metrics"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/fullrt"
	"github.com/multiformats/go-multiaddr"
	bsm "github.com/whyrusleeping/go-bs-measure"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	logging "github.com/ipfs/go-log"
)

var log = logging.Logger("whypfs")

var bootstrappers = []string{
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
}

var BootstrapPeers []peer.AddrInfo

func init() {

	for _, bsp := range bootstrappers {
		ma, err := multiaddr.NewMultiaddr(bsp)
		if err != nil {
			log.Errorf("failed to parse bootstrap address: ", err)
			continue
		}

		ai, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			log.Errorf("failed to create address info: ", err)
			continue
		}

		BootstrapPeers = append(BootstrapPeers, *ai)
	}
}

type Node struct {
	tracer trace.Tracer

	Dht      *dht.IpfsDHT
	Provider *batched.BatchProvidingSystem
	FullRT   *fullrt.FullRT
	Host     host.Host

	Datastore  datastore.Batching
	Blockstore blockstore.Blockstore
	StorageDir string

	Bitswap *bitswap.Bitswap

	Bwc *metrics.BandwidthCounter

	DB *gorm.DB

	Cfg *Config

	inflightCids   map[cid.Cid]uint
	inflightCidsLk sync.Mutex
}

type DeleteManyBlockstore interface {
	blockstore.Blockstore
	DeleteMany(context.Context, []cid.Cid) error
}

func (n *Node) KeyProviderFunc(ctx context.Context) (<-chan cid.Cid, error) {
	if n.Cfg.NoAnnounceContent {
		ch := make(chan cid.Cid)
		close(ch)
		return ch, nil
	}

	// TODO: pull this from a database with configurable announcement options
	return n.Blockstore.AllKeysChan(ctx)

}

type Config struct {
	Libp2pKeyFile     string
	ListenAddrs       []string
	AnnounceAddrs     []string
	DatastoreDir      string
	Blockstore        string
	NoBlockstoreCache bool
	NoAnnounceContent bool

	DatabaseConnString string

	NoLimiter     bool
	BitswapConfig BitswapConfig
	//LimitsConfig            Limits
	ConnectionManagerConfig ConnectionManager
}

type ConnectionManager struct {
	HighWater int
	LowWater  int
}

type BitswapConfig struct {
	MaxOutstandingBytesPerPeer int64
	TargetMessageSize          int
}

func Setup(ctx context.Context, cfg *Config) (*Node, error) {

	db, err := SetupDatabase(cfg.DatabaseConnString)
	if err != nil {
		return nil, err
	}

	peerkey, err := loadOrInitPeerKey(cfg.Libp2pKeyFile)
	if err != nil {
		return nil, err
	}

	ds, err := levelds.NewDatastore(cfg.DatastoreDir, nil)
	if err != nil {
		return nil, err
	}

	var rcm network.ResourceManager

	if cfg.NoLimiter || true {
		rcm, err = network.NullResourceManager, nil
		log.Warnf("starting node with no resource limits")
	} else {
		/*
			lim := cfg.GetLimiter()
			rcm, err = rcmgr.NewResourceManager(lim)
		*/
	}
	if err != nil {
		return nil, err
	}

	bwc := metrics.NewBandwidthCounter()

	cmgr, err := connmgr.NewConnManager(cfg.ConnectionManagerConfig.LowWater, cfg.ConnectionManagerConfig.HighWater)
	if err != nil {
		return nil, err
	}
	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(cfg.ListenAddrs...),
		libp2p.NATPortMap(),
		libp2p.ConnectionManager(cmgr),
		libp2p.Identity(peerkey),
		libp2p.BandwidthReporter(bwc),
		libp2p.DefaultTransports,
		libp2p.ResourceManager(rcm),
	}

	if len(cfg.AnnounceAddrs) > 0 {
		var addrs []multiaddr.Multiaddr
		for _, anna := range cfg.AnnounceAddrs {
			a, err := multiaddr.NewMultiaddr(anna)
			if err != nil {
				return nil, fmt.Errorf("failed to parse announce addr: %w", err)
			}
			addrs = append(addrs, a)
		}
		opts = append(opts, libp2p.AddrsFactory(func([]multiaddr.Multiaddr) []multiaddr.Multiaddr {
			return addrs
		}))
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}

	dhtopts := fullrt.DHTOption(
		//dht.Validator(in.Validator),
		dht.Datastore(ds),
		dht.BootstrapPeers(BootstrapPeers...),
		dht.BucketSize(20),
	)

	frt, err := fullrt.NewFullRT(h, dht.DefaultPrefix, dhtopts)
	if err != nil {
		return nil, xerrors.Errorf("constructing fullrt: %w", err)
	}

	ipfsdht, err := dht.New(ctx, h, dht.Datastore(ds))
	if err != nil {
		return nil, xerrors.Errorf("constructing dht: %w", err)
	}

	mbs, stordir, err := loadBlockstore(cfg.Blockstore, cfg.NoBlockstoreCache)
	if err != nil {
		return nil, err
	}

	blkst := mbs

	bsnet := bsnet.NewFromIpfsHost(h, frt)

	peerwork := cfg.BitswapConfig.MaxOutstandingBytesPerPeer
	if peerwork == 0 {
		peerwork = 5 << 20
	}

	bsopts := []bitswap.Option{
		bitswap.EngineBlockstoreWorkerCount(600),
		bitswap.TaskWorkerCount(600),
		bitswap.MaxOutstandingBytesPerPeer(int(peerwork)),
	}

	if tms := cfg.BitswapConfig.TargetMessageSize; tms != 0 {
		bsopts = append(bsopts, bitswap.WithTargetMessageSize(tms))
	}

	bsctx := metri.CtxScope(ctx, "estuary.exch")
	bswap := bitswap.New(bsctx, bsnet, blkst, bsopts...)

	nd := &Node{
		Dht:        ipfsdht,
		FullRT:     frt,
		Host:       h,
		Blockstore: mbs,
		//Lmdb:       lmdbs,
		Datastore:  ds,
		Bitswap:    bswap.(*bitswap.Bitswap),
		Bwc:        bwc,
		StorageDir: stordir,
		Cfg:        cfg,
		DB:         db,

		inflightCids: make(map[cid.Cid]uint),
	}

	provq, err := queue.NewQueue(context.Background(), "provq", ds)
	if err != nil {
		return nil, err
	}

	prov, err := batched.New(frt, provq,
		batched.KeyProvider(nd.KeyProviderFunc),
		batched.Datastore(ds),
	)
	if err != nil {
		return nil, xerrors.Errorf("setup batched provider: %w", err)
	}

	prov.Run() // TODO: call close at some point
	nd.Provider = prov

	return nd, nil

}

func loadOrInitPeerKey(kf string) (crypto.PrivKey, error) {
	data, err := ioutil.ReadFile(kf)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		k, _, err := crypto.GenerateEd25519Key(crand.Reader)
		if err != nil {
			return nil, err
		}

		data, err := crypto.MarshalPrivateKey(k)
		if err != nil {
			return nil, err
		}

		if err := ioutil.WriteFile(kf, data, 0600); err != nil {
			return nil, err
		}

		return k, nil
	}
	return crypto.UnmarshalPrivateKey(data)
}

func loadBlockstore(bscfg string, nocache bool) (blockstore.Blockstore, string, error) {
	bstore, dir, err := constructBlockstore(bscfg)
	if err != nil {
		return nil, "", err
	}

	ctx := metri.CtxScope(context.TODO(), "estuary.bstore")

	bstore = bsm.New("estuary.blks.base", bstore)

	if !nocache {
		cbstore, err := blockstore.CachedBlockstore(ctx, bstore, blockstore.CacheOpts{
			//HasBloomFilterSize:   512 << 20,
			//HasBloomFilterHashes: 7,
			HasARCCacheSize: 8 << 20,
		})
		if err != nil {
			return nil, "", err
		}
		bstore = &deleteManyWrap{cbstore}
	}

	mbs := bsm.New("estuary.repo", bstore)

	var blkst blockstore.Blockstore = mbs

	return blkst, dir, nil
}

/* format:
:lmdb:/path/to/thing
*/
func constructBlockstore(bscfg string) (DeleteManyBlockstore, string, error) {

	spec, params, path, err := parseBsCfg(bscfg)
	if err != nil {
		return nil, "", err
	}

	switch spec {
	/*
		case "lmdb":
			lmdbs, err := lmdb.Open(&lmdb.Options{
				Path:   path,
				NoSync: true,
			})
			if err != nil {
				return nil, path, err
			}
			return lmdbs, "", nil
	*/
	case "flatfs":
		sfs := "/repo/flatfs/shard/v1/next-to-last/3"
		if len(params) > 0 {
			parts := strings.Split(params[0], "=")
			switch parts[0] {
			case "type":
				switch parts[1] {
				case "estuary":
					// default
					sfs = "/repo/flatfs/shard/v1/next-to-last/3"
				case "go-ipfs":
					sfs = "/repo/flatfs/shard/v1/next-to-last/2"
				default:
					return nil, "", fmt.Errorf("unrecognized flatfs repo type in params: %s", parts[1])
				}
			}
		}
		sf, err := flatfs.ParseShardFunc(sfs)
		if err != nil {
			return nil, "", err
		}

		ds, err := flatfs.CreateOrOpen(path, sf, false)
		if err != nil {
			return nil, "", err
		}

		return &deleteManyWrap{blockstore.NewBlockstoreNoPrefix(ds)}, path, nil
		/*
			case "migrate":
				if len(params) != 2 {
					return nil, "", fmt.Errorf("migrate blockstore requires two params (%d given)", len(params))
				}

				from, _, err := constructBlockstore(params[0])
				if err != nil {
					return nil, "", fmt.Errorf("failed to construct source blockstore for migration: %w", err)
				}

				to, destPath, err := constructBlockstore(params[1])
				if err != nil {
					return nil, "", fmt.Errorf("failed to construct dest blockstore for migration: %w", err)
				}

				mgbs, err := migratebs.NewBlockstore(from, to, true)
				if err != nil {
					return nil, "", err
				}

				return mgbs, destPath, nil
		*/
	default:
		return nil, "", fmt.Errorf("unrecognized blockstore spec: %q", spec)
	}
}

func parseBsCfg(bscfg string) (string, []string, string, error) {
	if bscfg[0] != ':' {
		return "", nil, "", fmt.Errorf("cfg must start with colon")
	}

	var inParen bool
	var parenStart int
	var parenEnd int
	var end int
	for i := 1; i < len(bscfg); i++ {
		if inParen {
			if bscfg[i] == ')' {
				inParen = false
				parenEnd = i
			}
			continue
		}

		if bscfg[i] == '(' {
			inParen = true
			parenStart = i
		}

		if bscfg[i] == ':' {
			end = i
			break
		}
	}

	if parenStart == 0 {
		return bscfg[1:end], nil, bscfg[end+1:], nil
	}

	t := bscfg[1:parenStart]
	params := strings.Split(bscfg[parenStart+1:parenEnd], ",")

	return t, params, bscfg[end+1:], nil
}

type deleteManyWrap struct {
	blockstore.Blockstore
}

func (dmw *deleteManyWrap) DeleteMany(ctx context.Context, cids []cid.Cid) error {
	for _, c := range cids {
		if err := dmw.Blockstore.DeleteBlock(ctx, c); err != nil {
			return err
		}
	}

	return nil
}

func SetupDatabase(dbval string) (*gorm.DB, error) {
	db, err := openDB(dbval)
	if err != nil {
		return nil, err
	}

	db.AutoMigrate(Pin{})
	db.AutoMigrate(Object{})
	db.AutoMigrate(ObjRef{})

	return db, nil

}
func openDB(dbval string) (*gorm.DB, error) {
	parts := strings.SplitN(dbval, "=", 2)
	if len(parts) == 1 {
		return nil, fmt.Errorf("format for database string is 'DBTYPE=PARAMS'")
	}

	var dial gorm.Dialector
	switch parts[0] {
	case "sqlite":
		dial = sqlite.Open(parts[1])
	case "postgres":
		dial = postgres.Open(parts[1])
	default:
		return nil, fmt.Errorf("unsupported or unrecognized db type: %s", parts[0])
	}

	db, err := gorm.Open(dial, &gorm.Config{
		SkipDefaultTransaction: true,
	})
	if err != nil {
		return nil, err
	}

	sqldb, err := db.DB()
	if err != nil {
		return nil, err
	}

	sqldb.SetMaxIdleConns(80)
	sqldb.SetMaxOpenConns(99)
	sqldb.SetConnMaxIdleTime(time.Hour)

	return db, nil
}
