#include "test_expiring_cache.h"
#include "expiring_cache/expiring_cache.h"

int ExpiringCacheUT::ttl_ms = 30;

std::shared_ptr<ExpiringCache<uint64_t>> ec;

TEST_F(ExpiringCacheUT, Start)
{
    ec = std::make_shared<ExpiringCache<uint64_t>>(std::chrono::milliseconds(ExpiringCacheUT::ttl_ms));
    // ExpiringCache::timer_worker is now running
}

TEST_F(ExpiringCacheUT, GetEmpty)
{
    uint64_t val = 0;
    bool succ = ec->get(val);
    EXPECT_EQ(succ, false);
    EXPECT_EQ(val, 0);
}

TEST_F(ExpiringCacheUT, UpdateGet)
{
    uint64_t val1 = 1, val2 = 0;
    ec->update(val1);
    bool succ = ec->get(val2);
    EXPECT_EQ(succ, true);
    EXPECT_EQ(val2, 1);
}

TEST_F(ExpiringCacheUT, UpdateExpireGet)
{
    uint64_t val1 = 2, val2 = 0;
    ec->update(val1);

    std::this_thread::sleep_for(std::chrono::milliseconds(ExpiringCacheUT::ttl_ms + 1));

    bool succ = ec->get(val2);
    EXPECT_EQ(succ, false);
    EXPECT_EQ(val2, 0);
}

TEST_F(ExpiringCacheUT, MultiUpdate)
{
    uint64_t val1 = 3, val2 = 4, val3 = 0;
    std::function updater = [&](uint64_t val) {
        int loop = 100;
        while (loop--) {
            ec->update(val);
        }
    };
    std::thread th1(updater, val1);
    std::thread th2(updater, val2);

    th1.join();
    th2.join();
    bool succ = ec->get(val3);
    EXPECT_EQ(succ, true);
    EXPECT_EQ(val3 == 3 || val3 == 4, true);
    ec->update(5);
    succ = ec->get(val3);
    EXPECT_EQ(succ, true);
    EXPECT_EQ(val3, 5);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
