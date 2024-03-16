package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/yzimhao/trading_engine/utils"
	"xorm.io/xorm"
	"xorm.io/xorm/names"
)

// func to get storage map key
func getShardID(shard *ton.BlockIDExt) string {
	return fmt.Sprintf("%d|%d", shard.Workchain, shard.Shard)
}

func getNotSeenShards(ctx context.Context, api ton.APIClientWrapped, shard *ton.BlockIDExt, shardLastSeqno map[string]uint32) (ret []*ton.BlockIDExt, err error) {
	if no, ok := shardLastSeqno[getShardID(shard)]; ok && no == shard.SeqNo {
		return nil, nil
	}

	b, err := api.GetBlockData(ctx, shard)
	if err != nil {
		return nil, fmt.Errorf("get block data: %w", err)
	}

	parents, err := b.BlockInfo.GetParentBlocks()
	if err != nil {
		return nil, fmt.Errorf("get parent blocks (%d:%x:%d): %w", shard.Workchain, uint64(shard.Shard), shard.Shard, err)
	}

	for _, parent := range parents {
		ext, err := getNotSeenShards(ctx, api, parent, shardLastSeqno)
		if err != nil {
			return nil, err
		}
		ret = append(ret, ext...)
	}

	ret = append(ret, shard)
	return ret, nil
}

type Block struct {
	Id          int64     `xorm:"pk autoincr bigint"`
	Block       int64     `xorm:"bigint"`
	Hash        string    `xorm:"varchar(64)"`
	Number2     int64     `xorm:"bigint"`
	AllNumber   string    `xorm:"varchar(64)"`
	CreatedTime time.Time `xorm:"timestamp created" json:"create_time"`
}

var database *xorm.Engine

func DatabaseInit(driver, dsn string, show_sql bool, prefix string) (err error) {
	defer func() {
		if err != nil {
			panic(err)
		}
	}()

	if database == nil {
		conn, err := xorm.NewEngine(driver, dsn)
		if err != nil {
			return err
		}

		if prefix != "" {
			tbMapper := names.NewPrefixMapper(names.SnakeMapper{}, prefix)
			conn.SetTableMapper(tbMapper)

		}

		conn.DatabaseTZ = time.Local
		conn.TZLocation = time.Local

		if err := conn.Ping(); err != nil {
			return err
		}

		if show_sql {
			conn.ShowSQL(true)

		} else {

		}
		database = conn
	}
	return nil
}

func main() {

	DatabaseInit("mysql", "root:root@tcp(localhost:3306)/dice?charset=utf8&loc=Local", false, "")
	database.Sync2(new(Block))

	client := liteclient.NewConnectionPool()

	cfg, err := liteclient.GetConfigFromUrl(context.Background(), "https://ton.org/global.config.json")
	if err != nil {
		log.Fatalln("get config err: ", err.Error())
		return
	}

	// connect to mainnet lite servers
	err = client.AddConnectionsFromConfig(context.Background(), cfg)
	if err != nil {
		log.Fatalln("connection err: ", err.Error())
		return
	}

	// initialize ton api lite connection wrapper with full proof checks
	api := ton.NewAPIClient(client, ton.ProofCheckPolicySecure).WithRetry()
	api.SetTrustedBlockFromConfig(cfg)

	log.Println("checking proofs since config init block, it may take near a minute...")

	master, err := api.GetMasterchainInfo(context.Background())
	if err != nil {
		log.Fatalln("get masterchain info err: ", err.Error())
		return
	}

	// TIP: you could save and store last trusted master block (master variable data)
	// for faster initialization later using api.SetTrustedBlock

	log.Println("master proofs chain successfully verified, all data is now safe and trusted!")

	// bound all requests to single lite server for consistency,
	// if it will go down, another lite server will be used
	ctx := api.Client().StickyContext(context.Background())

	// storage for last seen shard seqno
	shardLastSeqno := map[string]uint32{}

	// getting information about other work-chains and shards of first master block
	// to init storage of last seen shard seq numbers
	firstShards, err := api.GetBlockShardsInfo(ctx, master)
	if err != nil {
		log.Fatalln("get shards err:", err.Error())
		return
	}
	for _, shard := range firstShards {
		shardLastSeqno[getShardID(shard)] = shard.SeqNo
	}

	re := regexp.MustCompile("[0-9]+")

	for {
		hash := master.FileHash
		h1 := base64.StdEncoding.EncodeToString(hash)
		seq := master.SeqNo
		numbers := re.FindAllString(string(h1), -1)

		data := Block{
			Hash:      h1,
			Block:     int64(seq),
			AllNumber: strings.Join(numbers, ""),
			Number2: func() int64 {
				a := utils.S2Int64(strings.Join(numbers, ""))
				return a % 100
			}(),
		}

		database.Insert(&data)
		log.Printf("block %d %s number: %s dice: %2d", seq, h1, data.AllNumber, data.Number2)

		master, err = api.WaitForBlock(master.SeqNo + 1).GetMasterchainInfo(ctx)
		if err != nil {
			log.Fatalln("get masterchain info err: ", err.Error())
			return
		}
	}
}
