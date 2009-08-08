all: 3sync

3sync: *.hs Network/AWS/*.hs
	ghc --make -o 3sync 3sync.hs

# test:
# 	cd tests; make

clean:
	rm -rf *.hi *.o 3sync 3diff dist
	find . -name '*.o' -o -name '*.hi' -exec rm '{}' ';'