# 倒排索引
 - 倒排索引(英文:Inverted Index)，是一种索引方法，常被用于全文检索系统中的一种单词文档映射结构
 - 现代搜索引擎绝大多数的索引都是基于倒排索引来进行构建的
 - 这源于在实际应用当中，用户在使用搜索引擎查找信息时往往只输入信息中的某个属性关键字，如一些用户不记得歌名，会输入歌词来查找歌名;输入某个节目内容片段来查找该节目等等

## 引入倒排索引
 - 在处理label时，使用正则表达式时（xx_xx_xx{lable1=}），引入了 倒排索引来解决 [postings.go](../../tsdb/index/postings.go) tsdb/index/postings.go:56
 - ```
   type MemPostings struct {
	    mtx     sync.RWMutex
	    m       map[string]map[string][]storage.SeriesRef
	    ordered bool
    }
   ```
 - 正排索引 通过 Id 找 label 
 - 倒排索引 通过 label 找 Id[]

## 索引过程
 - [head.go](../../tsdb/head.go)
 - ```
   func (h *Head) getOrCreateWithID(id chunks.HeadSeriesRef, hash uint64, lset labels.Labels) (*memSeries,   bool, error) {
	    s, created, err := h.series.getOrSet(hash, lset, func() *memSeries {
		  shardHash := uint64(0)
		  if h.opts.EnableSharding {
			  shardHash = labels.StableHash(lset)
		  }

		  return newMemSeries(lset, id, shardHash, h.opts.IsolationDisabled)
	    })
	    if err != nil {
          return nil, false, err
	    }
	    if !created {
          rurn s, false, nil
	    }

	    h.metrics.seriesCreated.Inc()
	    h.numSeries.Inc()
  
	    h.postings.Add(storage.SeriesRef(id), lset)
	    return s, true, nil
   }
   ```
 - 主要函数 tsdb/index/postings.go:377