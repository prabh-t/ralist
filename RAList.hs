-----------------------------------------------------------------------------
-- |
-- Module : RAList
-- Author : Prabhat Totoo 2011
--
-- Alternative representation of Haskell's widely used list
-- data structure based on Chris Okasaki's implementation of
-- random-access list. This representation is intended to expose
-- parallelism using a structure built around trees which is better
-- suited for parallel processing.
--
-- Many basic list functions found in the @Prelude@ and @Data.List@
-- modules are provided. Qualify uses of these function names with an 
-- alias for this module.
--
-- Parallel functions are marked /Par./ and the time complexity as /O(..)/.
-- 
-----------------------------------------------------------------------------

module RAList (
  -- * Types
  RAList (..),
  -- * Basic operations
  -- | A number of basic operations on the new structure are 
  -- implemented in order to support other main functions.
  cons, RAList.head, RAList.tail, empty, isEmpty, RAList.length,
  -- * Convertion operations
  -- | These are used for converting from/to standard list.
  fromDataList, toDataList, toDataList',
  -- * More advanced operations
  -- | which include lookup (search) by index, update, checking 
  -- if element is in RAList (elem), obtaining sub-RAList 
  -- from take, drop, filter and partition operations.
  RAList.lookup, update, RAList.elem, pelem, pelem',
  RAList.take, RAList.drop, RAList.filter, RAList.partition,
  -- * Map
  -- | RAList transformations.
  ralMap,pRalMap,pRalMap',
  -- * Fold
  RAList.fold, RAList.parfold, RAList.parfold',
  -- * Parallel strategies
  -- | Strategies are defined for Haskell's list in the module `Control.Parallel.Strategies`. 
  -- Following are probably the 2 mostly used strategies.
  evalRAList, parRAList,
  -- * Benchmark applications
  -- | The following are implemented so that a direct comparison can 
  -- be made with similar implementation in standard list found in 
  -- the StdList module.
  
  -- ** Sum
  RAList.sum, RAList.psum, RAList.psum',
  -- ** Factorial
  RAList.facto, RAList.pfacto,
  -- ** Sort
  RAList.quicksort,
  -- ** Histogram
  RAList.histo,
  -- ** Minimum
  pRalMin, pRalMin',
  -- ** Nub
  nub, nub1, nub2
  ) where
  import ParTree
  import Prelude hiding (head,tail,lookup,length,filter,take,drop,elem)
  import Prelude as Pre (head,tail,lookup,length,filter,take,drop)
  import System
  import System.IO
  import Data.List as List (foldr,foldl,foldl',sum,filter,reverse,map,or)
  import Data.Foldable hiding (minimum)
  import Control.DeepSeq
  import Control.Parallel
  import Control.Parallel.Strategies
  import GHC.Conc (numCapabilities)
  
  -- | RAList is the new data type to be used instead of list. It is 
  -- defined as a list of complete binary trees (implemented in the 
  -- module @ParTree@).
  --
  -- > data RAList a = RAL !Int ![Digit a]
  -- > data Digit a = Zero | One !(Tree a)
  -- > data Tree a = Leaf a | Node !Int !(Tree a) !(Tree a) -- Defined in ParTree module.
  --
  data RAList a = RAL !Int ![Digit a] deriving Show
  data Digit a = Zero | One !(Tree a) deriving Show

  -- Providing NFData instances for new data types defined above.
  instance (NFData a) => NFData (RAList a) where
    rnf (RAL s ds) = deepseq ds ()

  instance (NFData a) => NFData (Digit a) where
    rnf (Zero) = ()
    rnf (One t) = deepseq t ()

  -- These functions are not exported and used in this module only.
  -- Construct tree.
  -- First argument is the new element to add to the structure.
  consTree t [] = [One t]
  consTree t (Zero : ts) = One t : ts
  consTree t1 (One t2 : ts) = Zero : consTree (link t1 t2) ts

  -- Unconstruct tree.
  unconsTree [] = error "RAList: empty list"
  unconsTree [One t] = (t, [])
  unconsTree (One t : ts) = (t, Zero : ts)
  unconsTree (Zero : ts) = (t1, One t2 : ts')
      where (Node _ t1 t2, ts') = unconsTree ts
	  
  -- Take a list of `Digit` and return all the trees in a new list.
  filterTrees :: [Digit a] -> [Tree a]
  filterTrees ts = List.reverse $ checkT [] ts
    where
	  checkT ys [] = ys
	  checkT ys (Zero:xs) = checkT ys xs
	  checkT ys ((One t):xs) = checkT (t:ys) xs

  -- |/O(1)/ Similar to cons in list @(:)@. It prepends an element to the RAList.
  cons :: a -> RAList a -> RAList a
  cons x (RAL s ts) = RAL (s+1) (consTree (Leaf x) ts)

  -- |/O(1)/ Return the first element from the RAList.
  head :: RAList a -> a
  head (RAL s ts) = let (Leaf x, _) = unconsTree ts in x

  -- |/O(1)/ Return all elements, except the first.
  tail :: RAList a -> RAList a
  tail (RAL s ts) = let (_, ts') = unconsTree ts in RAL (s-1) ts'

  -- |An empty RAList where the size is 0 hence indicating that 
  -- the structure is empty.
  --
  -- > RAL 0 []
  empty :: RAList a
  empty = RAL 0 []

  -- |/O(1)/ Check if RAList is empty, that is, size @s@ equals to 0 in:
  --
  -- > RAL s ts
  isEmpty :: RAList a -> Bool
  isEmpty (RAL s ts) = s == 0 --or, we could do: null ts

  -- |/O(1)/ Return the number of elements in the RAList. The corresponding
  -- function @length@ in standard list has a time complexity of /O(n)/.
  length :: RAList a -> Int
  length (RAL s ts) = s

  -- | Take the first @n@ elements.
  take :: Int -> RAList a -> RAList a
  take n _      | n <= 0 =  empty
  take _ (RAL 0 []) = empty
  take n list = x `cons` RAList.take (n-1) xs
    where
	  x = RAList.head list
	  xs = RAList.tail list

  -- | Drop the first @n@ elements.
  drop :: Int -> RAList a -> RAList a
  drop n list     | n <= 0 = list
  drop _ (RAL 0 []) = empty
  drop n list = RAList.drop (n-1) xs
    where
	  xs = RAList.tail list
	  
  -- | Build a RAList from a list.
  fromDataList :: [a] -> RAList a
  fromDataList xs = List.foldl' (flip cons) empty (List.reverse xs)

  -- | Convert a RAList to a list.
  toDataList :: RAList a -> [a]
  toDataList list@(RAL s _) =
    if s == 0 then []
	else (RAList.head list) : toDataList (RAList.tail list)
	
  -- | /Par./ An alternative (parallel) implementation of `toDataList`.
  toDataList' :: NFData a => RAList a -> [a]
  toDataList' (RAL _ ts) = List.foldl (++) [] lists
    where
	  trees = filterTrees ts
	  lists = map treeToList trees `using` parList rdeepseq
	  
  -- |/O(log n)/ Lookup the RAList using the given index like @(!!)@ in 
  -- standard list. Index has to be in range, otherwise an error occurs.
  lookup :: Int -> RAList a -> a
  lookup i (RAL s ts) = look i ts
    where
      fail = error "RAList.lookup: bad subscript"

      look i [] = fail
      look i (Zero : ts) = look i ts
      look i (One t : ts) =
        if i < size t then lookTree i t else look (i - size t) ts

      lookTree 0 (Leaf x) = x
      lookTree i (Leaf x) = fail
      lookTree i (Node w t1 t2) =
        if i < w `div` 2 then lookTree i t1
        else lookTree (i - w `div` 2) t2

  -- |/O(log n)/ Update the element at the given index with a new value.
  update::Int -> a -> RAList a -> RAList a
  update i y (RAL s ts) = RAL s (upd i ts)
    where
      fail = error "RAList.update: bad subscript"

      upd i [] = fail
      upd i (Zero : ts) = Zero : upd i ts
      upd i (One t : ts) =
        if i < size t then One (updTree i t) : ts
        else One t : upd (i - size t) ts

      updTree 0 (Leaf x) = Leaf y
      updTree i (Leaf x) = fail
      updTree i (Node w t1 t2) =
        if i < w `div` 2 then Node w (updTree i t1) t2
        else Node w t1 (updTree (i - w `div` 2) t2)
		
  -- | Check for any occurrence of the given element in the structure. Seq implementation
  -- similar to `elem` from standard list.
  elem :: Eq a => a -> RAList a -> Bool
  elem x ts = 
    if RAList.length ts == 0 then False
	else
	  if x == RAList.head ts then True
      else RAList.elem x $ RAList.tail ts

  -- | /Par./ parallel implementation of `elem`.
  pelem:: Eq a => a -> RAList a -> Bool
  pelem x (RAL _ []) = False
  pelem x (RAL s (Zero : ts)) = pelem x (RAL s ts)
  pelem x (RAL s (One t : ts)) = l `par` r `seq` (l || r)
    where
	  l = treeElem x t
	  r = pelem x (RAL s ts)
	  
  -- | /Par./ improved parallel version.
  pelem' :: (Eq a, NFData a) => a -> RAList a -> Bool
  pelem' x (RAL _ ts) = List.or lists
    where
	  trees = filterTrees ts
	  lists = map (treeElem x) trees `using` parList rdeepseq

  -- | Return a new RAList with only those elements that match the predicate.
  filter::(a -> Bool) -> RAList a -> RAList a
  filter f ts =
    if RAList.length ts == 0 then empty
	else
	  if f x then x `cons` RAList.filter f xs
	  else RAList.filter f xs
	    where
	      x = RAList.head ts
	      xs = RAList.tail ts

  -- | Return 2 partitions of the given RAList. The first RAList in the tuple pair 
  -- satisfies the predicate, while the second does not.
  partition::(a -> Bool) -> RAList a -> (RAList a, RAList a)
  partition p (RAL s []) = (empty, empty)
  partition p list =
	if p x then (cons x yes,no)
	else (yes, cons x no)
		where
			(yes, no) = partition p xs
			x = RAList.head list
			xs = RAList.tail list

  -- HIGH-ORDER FUNCTIONS - Map and Fold

  -- | Sequential high-order map function which applies a function
  -- `f` to every element of the RAList.
  -- 
  -- > ralMap sqrt $ fromDataList [1,4,9]
  -- > RAL 3 [One (Leaf 1.0),One (Node 2 (Leaf 2.0) (Leaf 3.0))]
  ralMap::(a -> b) -> RAList a -> RAList b
  ralMap f ts =
    if RAList.length ts == 0 then empty
	else f (RAList.head ts) `cons` (ralMap f (RAList.tail ts))

  -- | /Par./ parallel implementation of map function on RAList, using `parRAList`.
  pRalMap :: Strategy b -> (a -> b) -> RAList a -> RAList b
  pRalMap strat f = (`using` parRAList strat) . ralMap f 
  
  -- | /Par./ another version that uses `treeMap`.
  pRalMap'::(NFData b) => (a -> b) -> RAList a -> RAList b
  pRalMap' f (RAL 0 []) = empty
  pRalMap' f (RAL n ts) = RAL n ts'
    where
	  ts' = map f' ts `using` parList rdeepseq
	  f' (Zero) = Zero
	  f' (One t) = One $ treeMap f t

  -- | Sequential fold on the RAList structure, same type signature as `foldl` (folds element from the left).
  --
  -- > fold (+) 0 $ fromDataList [1..5]
  fold :: (a -> b -> a) -> a -> RAList b -> a
  fold f z ts =
    if isEmpty ts then z
	else RAList.fold f (f z $ RAList.head ts) (RAList.tail ts)
	  
  -- | /Par./ parallel implementaion of fold function. The collection of trees
  -- in the RAList is folded in parallel and at tree level, more parallelism is 
  -- gained through the use of `treeFold`. A top level fold generates the final 
  -- result. The function `f` has to be both associative and commutative.
  parfold :: (NFData a) => (a -> a -> a) -> a -> RAList a -> a
  parfold f z (RAL _ ts) = res
    where
	  trees = filterTrees ts
	  res = Data.Foldable.foldl f z (map (treeFold f z) trees `using` parList rdeepseq)

  -- | /Par./ an alternative parallel version of fold on RAList where the collection 
  -- of trees are processed in order and parallelism is at sub-structure (tree level) only.
  parfold' :: (NFData a) => (a -> a -> a) -> a -> RAList a -> a
  parfold' f z (RAL _ ts) = pfold z ts
    where
	  pfold z' [] = z'
	  pfold z' (One t : xs) = pfold (f z' (treeFold f z t)) xs
	  pfold z' (Zero : xs) = pfold z' xs
	  
  -- STRATEGIES
  
  -- | Strategy combinator that walks over the RAList and applies the
  -- argument strategy `s` to every element. It generalises `parRAList`.
  --
  -- `seqRAList` is not given, but can be easily defined by
  --
  -- > seqRAList s = evalRAList (rseq `dot` s)
  evalRAList::Strategy a -> Strategy (RAList a)
  evalRAList s (RAL 0 []) = return empty
  evalRAList s ts = do x' <- s x
                       xs' <- evalRAList s xs
                       return (x' `cons` xs')
    where
	  x = RAList.head ts
	  xs = RAList.tail ts

  -- | `parRAList` is obtained by composing the element strategy `s` with `rpar`.
  --
  -- > parRAList s = evalRAList (rpar `dot` s)
  --
  -- Example
  --
  -- > ralist = fromDataList [1..5]
  -- > ralMap (+1) ralist `using` parRAList rdeepseq
  --
  -- Or
  --
  -- > pRalMap rdeepseq (+1) ralist
  parRAList::Strategy a -> Strategy (RAList a)
  parRAList s = evalRAList (rpar `dot` s)

  -- Benchmark Applications
  
  -- | Sequential `sum` function that uses `fold`.
  sum :: (Num a) => RAList a -> a
  sum ts = RAList.fold (+) 0 ts

  -- | /Par./ parallel `sum` function that uses `parfold`.
  psum :: (NFData a, Num a) => RAList a -> a
  psum ts = parfold (+) 0 ts
  
  -- | /Par./ parallel `sum` function that uses `parfold'`.
  psum' :: (NFData a, Num a) => RAList a -> a
  psum' ts = parfold' (+) 0 ts
  
  -- | Sequential `factorial` function that uses `fold`.
  facto :: Integer -> Integer
  facto x | x < 0 = error "facto: Negative number!"
          | x == 0 = 1
          | otherwise = RAList.fold (*) 1 $ fromDataList [1..x]

  -- | /Par./ parallel `factorial` function that uses `parfold`.
  pfacto :: Integer -> Integer
  pfacto x | x < 0 = error "facto: Negative number!"
           | x == 0 = 1
           | otherwise = parfold (*) 1 $ fromDataList [1..x]
	  
  -- | Sort a RAList of @n@ random numbers using the quicksort algorithm.
  quicksort :: (Ord a) => RAList a -> RAList a
  quicksort list = qsort list empty
      where
        qsort list rest =
		  if isEmpty list then rest
		  else
			let	x = RAList.head list
				xs = RAList.tail list
				(smalls,bigs) = partition (<x) xs --part (x,xs,empty,empty)
			in qsort smalls (cons x (qsort bigs rest))

  -- | The `histogram` function counts the occurrences of each integer in a list of @5n@ integers chosen randomly from @0..n-1@.
  histo :: Int -> [Int] -> RAList Int
  histo n randlist = Prelude.foldl inc (init n) randlist
    where
	  init 0 = empty
	  init n = cons 0 (init (n-1))
	  
	  inc ts i = update i ((RAList.lookup i ts)+1) ts

  -- | /Par./ Return the smallest integer from the RAList.
  pRalMin :: (Num a, Ord a) => RAList a -> a
  pRalMin xs = findMin h t
    where
	  h = RAList.head xs
	  RAL _ t = RAList.tail xs
  
  findMin :: (Num a, Ord a) => a -> [Digit a] -> a
  findMin x [] = x
  findMin x (Zero : ts) = min x (findMin x ts)
  findMin x (One t : ts) = min (treeMin t) (findMin x ts)
  
  -- | /Par./ improved parallel version. `min` is commutative i.e. min x y = min y x, 
  -- so it can be used in `fold`.
  pRalMin' :: (Num a, Ord a, NFData a) => RAList a -> a
  pRalMin' ralist@(RAL _ ts) = List.foldl' min x mins
    where
	  x = RAList.head ralist
	  trees = filterTrees ts
	  mins = map treeMin trees `using` parList rdeepseq

  -- | /Par./ The `nub` function removes duplicate elements from the RAList. Naive version.
  nub :: (Eq a) => RAList a -> [Digit a] -> RAList a
  nub acc [] = acc
  nub acc (Zero : ts) = nub acc ts
  nub acc (One t : ts) = nub (newacc t acc) ts
    where
	  newacc (Leaf n) ac@(RAL s xx) = if pelem n (RAL 0 xx) then ac else cons n ac
	  newacc (Node _ l r) ac = newacc r (newacc l ac)

  -- | nub-circular.
  nub1 :: (Eq a) => RAList a -> RAList a
  nub1 ts =
    if RAList.length ts == 0 then empty
	else x `cons` (nub1 (RAList.filter (/= x) xs))
      where
	    x = RAList.head ts
	    xs = RAList.tail ts

  -- | more advanced nub-circular. (Does not work).
  nub2 :: (Eq a) => RAList a -> RAList a
  nub2 ts = res
    where
	  res = build ts 0
	  
	  build (RAL 0 _) _ = empty	  
	  build ts' n | mem x res n = build xs n
	              | otherwise = x `cons` (build xs (n+1))
		where
	      x = RAList.head ts'
	      xs = RAList.tail ts'
	
	  mem _ _ 0 = False
	  mem x ts'' n | x == y = True
		           | otherwise = mem x ys (n-1)
		where
	      y = RAList.head ts''
	      ys = RAList.tail ts''
