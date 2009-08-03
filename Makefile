all: 3sync 3diff

3sync: *.hs
	ghc --make -o 3sync 3sync.hs

3diff: *.hs
	ghc --make -o 3diff 3diff.hs

# test:
# 	cd tests; make

clean:
	rm -rf *.hi *.o 3sync 3diff dist
