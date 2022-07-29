package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	pinner "github.com/application-research/go-pinmgr"
	"github.com/application-research/go-pinmgr/types"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-merkledag"
	echo "github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p-core/peer"
	"gorm.io/gorm"
)

const (
	ERR_INVALID_PINNING_STATUS = "ERR_INVALID_PINNING_STATUS"
)

// handleAddPin  godoc
// @Summary      Add and pin object
// @Description  This endpoint adds a pin to the IPFS daemon.
// @Tags         pinning
// @Produce      json
// @in           200,400,default  string  Token "token"
// @Param        cid   path  string  true  "cid"
// @Param        name  path  string  true  "name"
// @Router       /pinning/pins [post]
func (s *Server) handleAddPin(e echo.Context) error {
	ctx := e.Request().Context()

	var pin types.IpfsPin
	if err := e.Bind(&pin); err != nil {
		return err
	}

	var addrInfos []peer.AddrInfo
	for _, p := range pin.Origins {
		ai, err := peer.AddrInfoFromString(p)
		if err != nil {
			return err
		}

		addrInfos = append(addrInfos, *ai)
	}

	obj, err := cid.Decode(pin.Cid)
	if err != nil {
		return err
	}

	status, err := s.Node.pinContent(ctx, obj, pin.Name, addrInfos, 0, pin.Meta)
	if err != nil {
		return err
	}

	status.Pin.Meta = pin.Meta

	return e.JSON(http.StatusAccepted, status)
}

// handleListPins godoc
// @Summary      List all pin status objects
// @Description  This endpoint lists all pin status objects
// @Tags         pinning
// @Produce      json
// @Failure      400  {object}  util.HttpError
// @Failure      404  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /pinning/pins [get]
func (s *Server) handleListPins(e echo.Context) error {
	_, span := s.tracer.Start(e.Request().Context(), "handleListPins")
	defer span.End()

	qcids := e.QueryParam("cid")
	qname := e.QueryParam("name")
	qmatch := e.QueryParam("match")
	qstatus := e.QueryParam("status")
	qbefore := e.QueryParam("before")
	qafter := e.QueryParam("after")
	qlimit := e.QueryParam("limit")
	qreqids := e.QueryParam("requestid")

	q := s.Node.DB.Model(Pin{}).Order("created_at desc")

	if qcids != "" {
		var cids []DbCID
		for _, cstr := range strings.Split(qcids, ",") {
			c, err := cid.Decode(cstr)
			if err != nil {
				return err
			}
			cids = append(cids, DbCID{CID: c})
		}
		q = q.Where("cid in ?", cids)
	}

	if qname != "" {
		switch strings.ToLower(qmatch) {
		case "ipartial":
			q = q.Where("lower(name) like ?", fmt.Sprintf("%%%s%%", strings.ToLower(qname)))
		case "partial":
			q = q.Where("name like ?", fmt.Sprintf("%%%s%%", qname))
		case "iexact":
			q = q.Where("lower(name) = ?", strings.ToLower(qname))
		default: //exact
			q = q.Where("name = ?", qname)
		}
	}

	if qbefore != "" {
		beftime, err := time.Parse(time.RFC3339, qbefore)
		if err != nil {
			return err
		}
		q = q.Where("created_at <= ?", beftime)
	}

	if qafter != "" {
		aftime, err := time.Parse(time.RFC3339, qafter)
		if err != nil {
			return err
		}

		q = q.Where("created_at > ?", aftime)
	}

	if qreqids != "" {
		var ids []int
		for _, rs := range strings.Split(qreqids, ",") {
			id, err := strconv.Atoi(rs)
			if err != nil {
				return err
			}
			ids = append(ids, id)
		}
		q = q.Where("id in ?", ids)
	}

	lim := 10 // default from spec
	if qlimit != "" {
		limit, err := strconv.Atoi(qlimit)
		if err != nil {
			return err
		}
		lim = limit
	}

	pinStatuses := make(map[types.PinningStatus]bool)
	if qstatus != "" {
		statuses := strings.Split(qstatus, ",")
		for _, s := range statuses {
			ps := types.PinningStatus(s)
			switch ps {
			case types.PinningStatusQueued, types.PinningStatusPinning, types.PinningStatusPinned, types.PinningStatusFailed:
				pinStatuses[ps] = true
			default:
				return &HttpError{
					Code:    http.StatusBadRequest,
					Reason:  ERR_INVALID_PINNING_STATUS,
					Details: fmt.Sprintf("unrecognized pin status in query: %q", s),
				}
			}
		}
	}

	q, err := filterForStatusQuery(q, pinStatuses)
	if err != nil {
		return err
	}

	var count int64
	if err := q.Count(&count).Error; err != nil {
		return err
	}

	q.Limit(lim)

	var pins []Pin
	if err := q.Scan(&pins).Error; err != nil {
		return err
	}

	out := make([]*types.IpfsPinStatus, 0)
	for _, p := range pins {
		st, err := s.Node.pinStatus(p, nil)
		if err != nil {
			return err
		}
		out = append(out, st)
	}

	return e.JSON(http.StatusOK, types.IpfsListPinStatus{
		Count:   int(count),
		Results: out,
	})
}

func filterForStatusQuery(q *gorm.DB, statuses map[types.PinningStatus]bool) (*gorm.DB, error) {
	// TODO maybe we should move all these statuses to a status column in contents
	if len(statuses) == 0 || len(statuses) == 4 {
		return q, nil // if no status filter or all statuses are specified, return all pins
	}

	pinned := statuses[types.PinningStatusPinned]
	failed := statuses[types.PinningStatusFailed]
	pinning := statuses[types.PinningStatusPinning]
	queued := statuses[types.PinningStatusQueued]

	if len(statuses) == 1 {
		switch {
		case pinned:
			return q.Where("active and not failed and not pinning"), nil
		case failed:
			return q.Where("failed and not active and not pinning"), nil
		case pinning:
			return q.Where("pinning and not active and not failed"), nil
		default:
			return q.Where("not active and not pinning and not failed"), nil
		}
	}

	if len(statuses) == 2 {
		if pinned && failed {
			return q.Where("(active or failed) and not pinning"), nil
		}

		if pinned && queued {
			return q.Where("active and not failed and not pinning"), nil
		}

		if pinned && pinning {
			return q.Where("(active or pinning) and not failed"), nil
		}

		if pinning && failed {
			return q.Where("(pinning or failed) and not active"), nil
		}

		if pinning && queued {
			return q.Where("pinning and not active and not failed"), nil
		}

		if failed && queued {
			return q.Where("failed and not active and not pinning"), nil
		}
	}

	if !statuses[types.PinningStatusFailed] {
		return q.Where("not failed and (active or pinning)"), nil
	}

	if !statuses[types.PinningStatusPinned] {
		return q.Where("not active and (failed or pinning"), nil
	}

	if !statuses[types.PinningStatusPinning] {
		return q.Where("not pinning and (active or failed"), nil
	}
	return q.Where("active or pinning or failed"), nil
}

// handleGetPin  godoc
// @Summary      Get a pinned objects
// @Description  This endpoint returns a pinned object.
// @Tags         pinning
// @Produce      json
// @Param        requestid  path  string  true  "cid"
// @Router       /pinning/pins/{requestid} [get]
func (s *Server) handleGetPin(e echo.Context) error {
	id, err := strconv.Atoi(e.Param("requestid"))
	if err != nil {
		return err
	}

	var pin Pin
	if err := s.Node.DB.First(&pin, "id = ?", uint(id)).Error; err != nil {
		return err
	}

	st, err := s.Node.pinStatus(pin, nil)
	if err != nil {
		return err
	}

	return e.JSON(http.StatusOK, st)
}

// handleReplacePin godoc
// @Summary      Replace a pinned object
// @Description  This endpoint replaces a pinned object.
// @Tags         pinning
// @Produce      json
// @Param        id  path  string  true  "id"
// @Router       /pinning/pins/{id} [post]
func (s *Server) handleReplacePin(e echo.Context) error {

	ctx := e.Request().Context()
	id, err := strconv.Atoi(e.Param("requestid"))
	if err != nil {
		return err
	}

	var pin types.IpfsPin
	if err := e.Bind(&pin); err != nil {
		return err
	}

	var dbpin Pin
	if err := s.Node.DB.First(&dbpin, "id = ?", id).Error; err != nil {
		return err
	}
	/*
		if content.UserID != u.ID {
			return &util.HttpError{
				Code:    http.StatusUnauthorized,
				Message: util.ERR_NOT_AUTHORIZED,
			}
		}
	*/

	var addrInfos []peer.AddrInfo
	for _, p := range pin.Origins {
		ai, err := peer.AddrInfoFromString(p)
		if err != nil {
			return err
		}

		addrInfos = append(addrInfos, *ai)
	}

	obj, err := cid.Decode(pin.Cid)
	if err != nil {
		return err
	}

	status, err := s.Node.pinContent(ctx, obj, pin.Name, addrInfos, uint(id), pin.Meta)
	if err != nil {
		return err
	}

	return e.JSON(http.StatusAccepted, status)
}

// handleDeletePin godoc
// @Summary      Delete a pinned object
// @Description  This endpoint deletes a pinned object.
// @Tags         pinning
// @Produce      json
// @Param        requestid  path  string  true  "requestid"
// @Router       /pinning/pins/{requestid} [delete]
func (s *Server) handleDeletePin(e echo.Context) error {
	// TODO: need to cancel any in-progress pinning operation
	ctx := e.Request().Context()
	id, err := strconv.Atoi(e.Param("requestid"))
	if err != nil {
		return err
	}

	var pin Pin
	if err := s.Node.DB.First(&pin, "id = ?", id).Error; err != nil {
		return err
	}
	/*
		if content.UserID != u.ID {
			return &util.HttpError{
				Code:    401,
				Message: util.ERR_NOT_AUTHORIZED,
			}
		}
	*/

	// TODO: what if we delete a pin that was in progress?
	if err := s.Node.unpinContent(ctx, uint(id)); err != nil {
		return err
	}

	return e.NoContent(http.StatusAccepted)
}

func (nd *Node) unpinContent(ctx context.Context, contid uint) error {
	var pin Pin
	if err := nd.DB.First(&pin, "id = ?", contid).Error; err != nil {
		return err
	}

	objs, err := nd.objectsForPin(ctx, pin.ID)
	if err != nil {
		return err
	}

	if err := nd.DB.Delete(&Pin{ID: pin.ID}).Error; err != nil {
		return err
	}

	if err := nd.DB.Where("pin = ?", pin.ID).Delete(&ObjRef{}).Error; err != nil {
		return err
	}

	if err := nd.clearUnreferencedObjects(ctx, objs); err != nil {
		return err
	}

	for _, o := range objs {
		// TODO: this is safe, but... slow?
		if _, err := nd.deleteIfNotPinned(ctx, o); err != nil {
			return err
		}
	}
	return nil
}

func (nd *Node) pinContent(ctx context.Context, obj cid.Cid, name string, peers []peer.AddrInfo, replace uint, meta map[string]interface{}) (*types.IpfsPinStatus, error) {

	var metab string
	if meta != nil {
		b, err := json.Marshal(meta)
		if err != nil {
			return nil, err
		}
		metab = string(b)
	}

	pin := Pin{
		Cid: DbCID{obj},

		Name:   name,
		Active: false,

		Pinning: true,
		PinMeta: metab,
	}
	if err := nd.DB.Create(&pin).Error; err != nil {
		return nil, err
	}

	nd.addPinToQueue(pin, peers, replace)

	return nd.pinStatus(pin, nil)
}

func (nd *Node) addPinToQueue(pin Pin, peers []peer.AddrInfo, replace uint) {

	op := &pinner.PinningOperation{
		ContId:  pin.ID,
		Obj:     pin.Cid.CID,
		Name:    pin.Name,
		Peers:   peers,
		Started: pin.CreatedAt,
		Status:  "queued",
		Replace: replace,
	}

	nd.pinLk.Lock()
	// TODO: check if we are overwriting anything here
	nd.pinJobs[pin.ID] = op
	nd.pinLk.Unlock()

	nd.PinMgr.Add(op)
}

func (nd *Node) pinDelegatesForPin(pin Pin) []string {
	var out []string
	for _, a := range nd.Host.Addrs() {
		out = append(out, fmt.Sprintf("%s/p2p/%s", a, nd.Host.ID()))
	}

	return out
}

func (nd *Node) pinStatus(pin Pin, origins []*peer.AddrInfo) (*types.IpfsPinStatus, error) {
	delegates := nd.pinDelegatesForPin(pin)

	nd.pinLk.Lock()
	po, ok := nd.pinJobs[pin.ID]
	nd.pinLk.Unlock()
	if !ok {
		meta := make(map[string]interface{}, 0)
		if pin.PinMeta != "" {
			if err := json.Unmarshal([]byte(pin.PinMeta), &meta); err != nil {
				log.Warnf("pin %d has invalid pinmeta: %s", pin.ID, err)
			}
		}

		originStrs := make([]string, 0)
		for _, o := range origins {
			ai, err := peer.AddrInfoToP2pAddrs(o)
			if err == nil {
				for _, a := range ai {
					originStrs = append(originStrs, a.String())
				}
			}
		}

		ps := &types.IpfsPinStatus{
			Requestid: fmt.Sprintf("%d", pin.ID),
			Status:    types.PinningStatusPinning,
			Created:   pin.CreatedAt,
			Pin: types.IpfsPin{
				Cid:     pin.Cid.CID.String(),
				Name:    pin.Name,
				Meta:    meta,
				Origins: originStrs,
			},
			Delegates: delegates,
			Info:      make(map[string]interface{}, 0), // TODO: all sorts of extra info we could add...
		}

		if pin.Active {
			ps.Status = types.PinningStatusPinned
		}

		if pin.Failed {
			ps.Status = types.PinningStatusFailed
		}
		return ps, nil
	}

	status := po.PinStatus()
	status.Delegates = delegates
	return status, nil
}

func (nd *Node) doPinning(ctx context.Context, op *pinner.PinningOperation, cb pinner.PinProgressCB) error {
	ctx, span := nd.tracer.Start(ctx, "doPinning")
	defer span.End()

	connectedToAtLeastOne := false
	for _, pi := range op.Peers {
		if err := nd.Host.Connect(ctx, pi); err != nil && nd.Host.ID() != pi.ID {
			log.Warnf("failed to connect to origin node for pinning operation: %s", err)
		} else {
			//	Check if it's trying to connect to itself since we only want to check if the
			//	the connection is between the host and the external/other peers.
			connectedToAtLeastOne = true
		}
	}

	//	If it can't connect to any legitimate provider peers, then we fail the entire operation.
	if !connectedToAtLeastOne {
		log.Errorf("unable to connect to any of the provider peers for pinning operation")
		return nil
	}

	bserv := blockservice.New(nd.Blockstore, nd.Bitswap)
	dserv := merkledag.NewDAGService(bserv)

	dsess := merkledag.NewSession(ctx, dserv)

	if err := nd.addDatabaseTrackingToPin(ctx, op.ContId, dsess, nd.Blockstore, op.Obj, cb); err != nil {
		return err
	}

	if op.Replace > 0 {
		if err := nd.removePin(ctx, op.Replace); err != nil {
			log.Infof("failed to remove content in replacement: %d", op.Replace)
		}
	}

	// this provide call goes out immediately
	if err := nd.FullRT.Provide(ctx, op.Obj, true); err != nil {
		log.Warnf("provider broadcast failed: %s", err)
	}

	// this one adds to a queue
	if err := nd.Provider.Provide(op.Obj); err != nil {
		log.Warnf("providing failed: %s", err)
	}

	return nil
}

func (nd *Node) removePin(ctx context.Context, pinid uint) error {
	var pin Pin
	if err := nd.DB.First(&pin, "id = ?", pinid).Error; err != nil {
		return err
	}

	objs, err := nd.objectsForPin(ctx, pin.ID)
	if err != nil {
		return err
	}

	if err := nd.DB.Delete(&Pin{ID: pin.ID}).Error; err != nil {
		return err
	}

	if err := nd.DB.Where("pin = ?", pin.ID).Delete(&ObjRef{}).Error; err != nil {
		return err
	}

	if err := nd.clearUnreferencedObjects(ctx, objs); err != nil {
		return err
	}

	for _, o := range objs {
		// TODO: this is safe, but... slow?
		if _, err := nd.deleteIfNotPinned(ctx, o); err != nil {
			return err
		}
	}

	return nil
}

func (nd *Node) clearUnreferencedObjects(ctx context.Context, objs []*Object) error {
	var ids []uint
	for _, o := range objs {
		ids = append(ids, o.ID)
	}
	nd.blocksLk.Lock()
	defer nd.blocksLk.Unlock()

	if err := nd.DB.Where("(?) = 0 and id in ?",
		nd.DB.Model(ObjRef{}).Where("object = objects.id").Select("count(1)"), ids).
		Delete(Object{}).Error; err != nil {
		return err
	}

	return nil
}

func (nd *Node) objectsForPin(ctx context.Context, pinid uint) ([]*Object, error) {
	var objects []*Object
	if err := nd.DB.Model(ObjRef{}).Where("pin = ?", pinid).
		Joins("left join objects on obj_refs.object = objects.id").
		Scan(&objects).Error; err != nil {
		return nil, err
	}

	return objects, nil
}

func (nd *Node) deleteIfNotPinned(ctx context.Context, o *Object) (bool, error) {
	ctx, span := nd.tracer.Start(ctx, "deleteIfNotPinned")
	defer span.End()

	nd.blocksLk.Lock()
	defer nd.blocksLk.Unlock()

	return nd.deleteIfNotPinnedLock(ctx, o)
}
func (nd *Node) deleteIfNotPinnedLock(ctx context.Context, o *Object) (bool, error) {
	ctx, span := nd.tracer.Start(ctx, "deleteIfNotPinnedLock")
	defer span.End()

	var objs []Object
	if err := nd.DB.Limit(1).Model(Object{}).Where("id = ? OR cid = ?", o.ID, o.Cid).Find(&objs).Error; err != nil {
		return false, err
	}
	if len(objs) == 0 {
		return true, nd.Blockstore.DeleteBlock(ctx, o.Cid.CID)
	}
	return false, nil
}
