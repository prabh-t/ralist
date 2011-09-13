-----------------------------------------------------------------------------
-- |
-- Module : SeqPerfComparison.hs
-- Author : Prabhat Totoo 2011
--
-- Sequential performance comparison between RAList and standard list.
-- The operations include a number of benchmark applications.
-----------------------------------------------------------------------------
import RAList
import StdList
import System
import System.IO
import System.Random
import Data.Time.Clock
import Text.Printf
import Data.List as List (take,sum,minimum,elem)
import Control.DeepSeq

printTime name start = do
  ct <- getCurrentTime
  putStrLn $ printf "%s: %s" name (show $ diffUTCTime ct start)
  
randomlist n = List.take (5 * n) $ randomRs (0,n-1) (mkStdGen n)

main = do
	args <- getArgs
	let n = read (args!!0) :: Int
	let list = (randomlist n)::[Int]
	let ralist = fromDataList list
	deepseq list $ return ()
	deepseq ralist $ return ()
	let z = read (args!!1) :: Int
	--run_bench n list ralist z
	ralist_cons_cost n
	list_cons_cost n
	run_bench n list ralist 1
	run_bench n list ralist 2
	run_bench n list ralist 3
	run_bench n list ralist 4
	
run_bench n list ralist 1 = bench_lookup (n `div` 2) list ralist
run_bench n list ralist 2 = bench_update (n `div` 2) list ralist
run_bench n list ralist 3 = bench_qsort list ralist
run_bench n list ralist 4 = bench_histo n list ralist

ralist_cons_cost n = do
	--print "ralist cons cost"
	--print n
	ct <- getCurrentTime
	let ralist = fromDataList [1..n]
	deepseq ralist $ return ()
	printTime "RAList" ct
	
list_cons_cost n = do
	--print "list cons cost"
	--print n
	ct <- getCurrentTime
	let list = [1..n]
	deepseq list $ return ()
	printTime "List" ct	
	
bench_lookup i list ralist = do
	print "lookup"
	ct <- getCurrentTime
	print $ list !! i
	printTime "List.!!" ct
	hFlush stdout
	ct <- getCurrentTime
	print $ RAList.lookup i ralist
	printTime "RAList.lookup" ct

bench_update i list ralist = do
	print "update"
	ct <- getCurrentTime
	let res1 = StdList.update i 0 list
	deepseq res1 $ return ()
	printTime "StdList.update" ct
	hFlush stdout
	ct <- getCurrentTime
	let res2 = RAList.update i 0 ralist
	deepseq res2 $ return ()
	printTime "RAList.update" ct

bench_qsort list ralist = do
	print "qsort"
	ct <- getCurrentTime
	let res1 = StdList.qsort list
	deepseq res1 $ return ()
	printTime "StdList.qsort" ct
	hFlush stdout
	ct <- getCurrentTime
	let res2 = RAList.quicksort ralist
	deepseq res2 $ return ()
	printTime "RAList.quicksort" ct

bench_histo n list ralist = do
	print "histo"
	ct <- getCurrentTime
	let res1 = StdList.histo n list
	deepseq res1 $ return ()
	printTime "StdList.histo:" ct
	hFlush stdout
	ct <- getCurrentTime
	let res2 = RAList.histo n list
	deepseq res2 $ return ()
	printTime "RAList.histo:" ct
