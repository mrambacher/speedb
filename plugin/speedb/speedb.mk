// TODO: ADD Speedb's Copyright Notice !!!!!

speedb_SOURCES = 							  																	\
										speedb_registry.cc														\
										paired_filter/speedb_paired_bloom.cc					\
										paired_filter/speedb_paired_bloom_internal.cc	\

speedb_HEADERS = 																  								\
										paired_filter/speedb_paired_bloom.h						\

speedb_FUNC = register_SpdbPairedBloomFilter

speedb_TESTS = 																										\
     speedb_customizable_test.cc																	\
		 paired_filter/speedb_db_bloom_filter_test.cc									\

speedb_customizable_test: plugin/speedb/speedb_customizable_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

speedb_db_bloom_filter_test: plugin/speedb/paired_filter/speedb_db_bloom_filter_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)