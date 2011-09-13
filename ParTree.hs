--{-# LANGUAGE DeriveTraversable, DeriveFoldable, DeriveFunctor, CPP, BangPatterns #-}
-----------------------------------------------------------------------------
-- |
-- Module : ParTree
-- Author : Prabhat Totoo 2011
--
-- A binary leaf tree data structure with parallel operations.
-- 
-- Elements are placed on leaves and the inner nodes store the size
-- of the subtree beneath it.
-----------------------------------------------------------------------------

module ParTree (
  -- * Type Definition
  Tree (..),
  -- * Basics
  size,
  link,
  -- * Parallel operations
  -- | All operations that follow are parallel implementations.
  treeToList,
  treeMap,
  treeFold,
  treeElem,
  treeReverse,
  treeMin
  ) where
  
	import Data.Foldable
	import Data.Traversable
	import Control.DeepSeq
	import Control.Parallel
	import Control.Parallel.Strategies

	-- | New data type for Tree with strictness annotations in the definition.
    --
	-- > data Tree a = Leaf a | Node !Int !(Tree a) !(Tree a)
	--	
	-- For example, a tree representation for the list [1,2,3,4]
	--
	-- > myTree = (Node 4 (Node 2 (Leaf 1) (Leaf 2)) (Node 2 (Leaf 3) (Leaf 4)))
	--
	data Tree a = Leaf a | Node !Int !(Tree a) !(Tree a) deriving Show --(Traversable , Foldable, Functor, Show, Eq)

	-- | This allows for Tree to be evaluated to normal form.
	instance (NFData a) => NFData (Tree a) where
	  rnf (Leaf x) = deepseq x ()
	  rnf (Node s t1 t2) = deepseq (s,t1,t2) ()
  
	-- | Find the size of a tree.
	size :: Tree a -> Int
	size (Leaf _) = 1
	size (Node s _ _) = s
	
	-- | Link two trees, one to the left and one to the right, making a node.
	link :: Tree a -> Tree a -> Tree a
	link t1 t2 = Node (size t1 + size t2) t1 t2

	-- | Convert a tree to list.
	treeToList :: Tree a -> [a]
	treeToList (Leaf x) = [x]
	treeToList (Node _ l r) = runEval $ do
									left <- rpar $ treeToList l
									right <- rseq $ treeToList r
									return (left ++ right)

	-- | Map a function `f` to each element of the tree in parallel, producing a
	-- new tree. Similar to `map` function in standard and `parMap` for its parallel 
	-- equivalent.
	treeMap :: (a -> b) -> Tree a -> Tree b
	treeMap f (Leaf x) = Leaf (f x)
	treeMap f (Node _ l r) = runEval $ do
									left <- rpar $ treeMap f l
									right <- rseq $ treeMap f r
									return (link left right)

	-- | Parallel `fold` function on tree. The first function passed as argument
	-- needs to be both associative and commutative, e.g. sum and product operators.
	-- Both have type def @Num a => a -> a -> a@
	treeFold :: (NFData a) => (a -> a -> a) -> a -> Tree a -> a
	treeFold f z (Leaf x) = f z x
	-- using `par` and `seq` primitives
	--treeFold f z (Node s l r) = if s > 10000 then l `par` r `seq` f l r
	--							  else f l r
	-- using `runEval` from new strategies (it fixes issue with space leak)
	treeFold f z (Node s l r) = if s > 10000 then runEval $ do
									left' <- rpar left
									right' <- rseq right
									return (f left' right')
								  else (f left right)
	  where
		left = treeFold f z l
		right = treeFold f z r
		
	-- | Check if there is any occurrence of the element in the tree. 
	-- Search proceeds by inspecting subtrees e.g. left and right
	-- branches in parallel.
	treeElem :: (Eq a) => a -> Tree a -> Bool
	treeElem e (Leaf x) = if x == e then True else False
	treeElem e (Node _ l r) = runEval $ do
									left <- rpar $ treeElem e l
									right <- rseq $ treeElem e r
									return (left || right)
									
	-- | Reverse all the elements in the tree.
	treeReverse :: Tree a -> Tree a
	treeReverse (Leaf x) = (Leaf x)
	treeReverse (Node _ l r) = runEval $ do
									left <- rpar $ treeReverse r
									right <- rseq $ treeReverse l
									return (link left right)

	-- | Return the smallest element from the tree.
	treeMin :: (Ord a) => Tree a -> a
	treeMin (Leaf x) = x
	treeMin (Node _ l r) = runEval $ do
									left <- rpar $ treeMin l
									right <- rseq $ treeMin r
									return (min left right)
