all: 3sync 3put

3sync: 3sync.hs Network/AWS/*.hs
	ghc --make -o 3sync 3sync.hs

3put: 3put.hs Network/AWS/*.hs
	ghc --make -o 3put 3put.hs

# test:
# 	cd tests; make

clean:
	rm -rf *.hi *.o 3sync 3diff dist
	find . -name '*.o' -o -name '*.hi' -exec rm '{}' ';'
