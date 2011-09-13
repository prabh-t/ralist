-----------------------------------------------------------------------------
-- |
-- Module : ParPerfComparison.hs
-- Author : Prabhat Totoo 2011
--
-- Parallel performance comparison between RAList and standard list.
--
-----------------------------------------------------------------------------
import RAList
import StdList
import System
import System.IO
import System.Random
import Data.Time.Clock
import Text.Printf
import Data.List as List
import Control.DeepSeq
import Control.Parallel
import Control.Parallel.Strategies

printTime name start = do
  ct <- getCurrentTime
  putStrLn $ printf "%s: %s" name (show $ diffUTCTime ct start)
  
main = do
	args <- getArgs
	let n = read (args!!0) :: Int
	let list = List.take n [1..]::[Int]
	let ralist = fromDataList list
	deepseq list $ return ()
	deepseq ralist $ return ()
	let z = read (args!!1) :: Int
	run_bench list ralist z

run_bench list ralist 1 = par_map list ralist
run_bench list ralist 2 = par_sum list ralist
run_bench list ralist 3 = par_min list ralist
run_bench list ralist 4 = par_elem list ralist
		
par_map list ralist = do
	print "Parallel map"
	ct <- getCurrentTime
	let res1 = parMap rdeepseq (\x->x^2) list
	deepseq res1 $ return ()
	printTime "1.parMap" ct
	hFlush stdout
	ct <- getCurrentTime
	let res2 = RAList.pRalMap rdeepseq (\x->x^2) ralist
	deepseq res2 $ return ()
	printTime "2.RAList.pRalMap" ct
	hFlush stdout
	ct <- getCurrentTime
	let res3 = RAList.pRalMap' (\x->x^2) ralist
	deepseq res3 $ return ()
	printTime "3.RAList.pRalMap'" ct
	
par_sum list ralist = do
	print "Parallel sum"
	ct <- getCurrentTime
	print $ StdList.parsum list
	printTime "1.StdList.parsum" ct
	hFlush stdout
	ct <- getCurrentTime
	print $ RAList.psum ralist
	printTime "2.RAList.psum" ct
	hFlush stdout
	ct <- getCurrentTime
	print $ RAList.psum' ralist
	printTime "3.RAList.psum'" ct

par_min list ralist = do
	print "Parallel min"
	ct <- getCurrentTime
	print $ StdList.parmin list
	printTime "1.StdList.parmin" ct
	hFlush stdout
	ct <- getCurrentTime
	print $ RAList.pRalMin ralist
	printTime "2.RAList.pRalMin" ct
	hFlush stdout
	ct <- getCurrentTime
	print $ RAList.pRalMin' ralist
	printTime "3.RAList.pRalMin'" ct
	
par_elem list ralist = do
	print "Parallel elem"
	ct <- getCurrentTime
	print $ StdList.parelem 0 list
	printTime "1.StdList.parelem" ct
	hFlush stdout
	ct <- getCurrentTime
	print $ RAList.pelem 0 ralist
	printTime "2.RAList.pelem" ct
	hFlush stdout
	ct <- getCurrentTime
	print $ RAList.pelem' 0 ralist
	printTime "3.RAList.pelem'" ct
