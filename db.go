package main

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
)

type Pin struct {
	ID        uint `gorm:"primarykey"`
	CreatedAt time.Time
	Name      string
	Cid       DbCID
	Active    bool
	Failed    bool
	Pinning   bool
	Size      int64
	Recursive bool
	PinMeta   string `json:"pinMeta"`
}

type Object struct {
	ID   uint  `gorm:"primarykey"`
	Cid  DbCID `gorm:"index"`
	Size int
}

type ObjRef struct {
	ID      uint `gorm:"primarykey"`
	Content uint `gorm:"index:,option:CONCURRENTLY"`
	Object  uint `gorm:"index:,option:CONCURRENTLY"`
}

type DbCID struct {
	CID cid.Cid
}

func (dbc *DbCID) Scan(v interface{}) error {
	b, ok := v.([]byte)
	if !ok {
		return fmt.Errorf("dbcids must get bytes!")
	}

	if len(b) == 0 {
		return nil
	}

	c, err := cid.Cast(b)
	if err != nil {
		return err
	}

	dbc.CID = c
	return nil
}

func (dbc DbCID) Value() (driver.Value, error) {
	return dbc.CID.Bytes(), nil
}

func (dbc DbCID) MarshalJSON() ([]byte, error) {
	return json.Marshal(dbc.CID.String())
}

func (dbc *DbCID) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	c, err := cid.Decode(s)
	if err != nil {
		return err
	}

	dbc.CID = c
	return nil
}
