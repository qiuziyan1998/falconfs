# contrib/falcon/Makefile

PG_CFLAGS = -Wno-declaration-after-statement

MODULE_big = falcon
SUBDIRS = . metadb utils distributed_backend transaction control dir_path_shmem connection_pool

falcon_srcdir = .
CXX_OBJS += \
	$(patsubst $(falcon_srcdir)/%.cpp,%.o,$(foreach dir,$(SUBDIRS), $(sort $(wildcard $(falcon_srcdir)/$(dir)/*.cpp))))
OBJS += \
	$(patsubst $(falcon_srcdir)/%.c,%.o,$(foreach dir,$(SUBDIRS), $(sort $(wildcard $(falcon_srcdir)/$(dir)/*.c))))
OBJS += $(CXX_OBJS)

REMOTE_CONNECTION_DEF_DIR = $(falcon_srcdir)/../../../../remote_connection_def
CONNECTION_POOL_DIR = $(falcon_srcdir)/connection_pool

FBS_DIR = $(shell find $(REMOTE_CONNECTION_DEF_DIR) -type d | grep -E '/fbs$$')
FBS_FILE = $(wildcard $(FBS_DIR)/*.fbs)
FBS_HEAD = $(patsubst $(REMOTE_CONNECTION_DEF_DIR)/%.fbs, $(CONNECTION_POOL_DIR)/%_generated.h, $(FBS_FILE))

all: $(FBS_HEAD) $(OBJS)

$(OBJS): $(FBS_HEAD)

$(CXX_OBJS): %.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -faligned-new -c $< -o $@

$(FBS_HEAD): $(FBS_FILE)
	mkdir -p $(CONNECTION_POOL_DIR)/fbs
	flatc --cpp -o $(CONNECTION_POOL_DIR)/fbs/ $(FBS_FILE)


PGFILEDESC = "falcon"

PG_CPPFLAGS = -I$(libpq_srcdir) -I./include -I./brpc_comm_adapter/proto -I./connection_pool/fbs \
			  -I$(REMOTE_CONNECTION_DEF_DIR) \
			  -I/usr/local/include \
			  -I$(falcon_srcdir)/../../../../falcon/third_party/glog/include \
			  -I$(falcon_srcdir)/../../../../third_party
SHLIB_LINK_INTERNAL =  -L../../src/interfaces/libpq -Wl,--no-as-needed -lpq -lstdc++ \
					-lz -Wl,--as-needed -lpthread -ldl

EXTENSION = falcon
DATA = falcon--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/falcon
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
