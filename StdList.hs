-----------------------------------------------------------------------------
-- |
-- Module : StdList
-- Author : Prabhat Totoo 2011
--
-- Functions and benchmark applications based on standard list.
-- 
-----------------------------------------------------------------------------

module StdList (
  -- * Seq. functions
  -- | Sequential operations on standard list.
  histo, histo', qsort, update,
  -- * Par. functions
  -- | Parallel operations on standard list.
  parsum, parmin, parelem
  ) where
	import Data.List
	import Control.DeepSeq
	import Control.Parallel
	import Control.Parallel.Strategies

	-- Seq. functions
	histo :: Int -> [Int] -> [Int]
	histo n randlist = map (\i ->length $ filter (== i) randlist) [0..n-1]

	histo' :: Int -> [Int] -> [Int]
	histo' n randlist = foldl inc (replicate n 0) randlist
		where
			inc xs i = a ++ [y] ++ tail b
				where
					y = head b + 1
					(a,b) = splitAt i xs    

	qsort :: Ord a => [a] -> [a]
	qsort [] = []
	qsort [x] = [x]
	qsort (x:xs) = losort ++ (x:hisort)
		where
			losort = qsort [y|y<-xs,y<x]
			hisort = qsort [y|y<-xs,y>=x]
			
	update::Int -> a -> [a] -> [a]
	update i x list = take i list ++ [x] ++ drop i list
	
	-- Par. functions
	parsum :: Num a => [a] -> a
	parsum []  = 0
	parsum [x] = x
	parsum xs = left `par` (right `pseq` (left + right))
		where
			left = parsum $ take half xs
			right = parsum $ drop half xs
			half = length xs `div` 2

	parmin :: (Ord a) => [a] -> a
	parmin xs  | cutoff    = minimum xs
			   | otherwise = left `par` (right `pseq` min left right)
			   where
					left = parmin $ take half xs
					right = parmin $ drop half xs
					half = length xs `div` 2
					cutoff = length xs < 100

	parelem :: (Eq a) => a -> [a] -> Bool
	parelem x xs  | cutoff    = elem x xs
			      | otherwise = left `par` (right `pseq` left || right)
			   where
					left = parelem x $ take half xs
					right = parelem x $ drop half xs
					half = length xs `div` 2
					cutoff = length xs < 100