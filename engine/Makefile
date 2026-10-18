# make       -> build/engine
# make test  -> build and run engine tests

CXX      ?= g++
CXXFLAGS ?= -std=c++17 -O2
INCLUDES := -I./vendor -I./src

LIB_SRCS := src/engine.cpp src/wal.cpp src/disk/pager.cpp src/schema/schema.cpp \
            src/storage/btree.cpp src/storage/secondary_index.cpp \
            src/storage/serializer.cpp src/storage/table_store.cpp
TESTS    := $(patsubst tests/%.cpp,build/%,$(wildcard tests/test_*.cpp))

.PHONY: all test clean

all: build/engine

build/engine: main.cpp $(LIB_SRCS)
	@mkdir -p build
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@

build/test_%: tests/test_%.cpp $(LIB_SRCS)
	@mkdir -p build
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@

test: $(TESTS)
	@for t in $(TESTS); do echo "== $$t"; ./$$t || exit 1; done

clean:
	rm -rf build
