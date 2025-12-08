#include "test_utils.h"

extern "C" {
#include "utils/utils_standalone.h"
#include "metadb/foreign_server.h"
}

TEST_F(ConnectionUT, ParseIp)
{
    const char *str = "{127.0.0.1,127.0.0.2}";
    auto stringArray = parse_text_array_direct(str);
    EXPECT_EQ(stringArray.count, 2);
    // std::cout << "stringArray.elements[0] = " << stringArray.elements[0] << std::endl;
    // std::cout << "stringArray.elements[1] = " << stringArray.elements[1] << std::endl;
    EXPECT_EQ(strcmp(stringArray.elements[0], "127.0.0.1"), 0);
    EXPECT_EQ(strcmp(stringArray.elements[1], "127.0.0.2"), 0);
}

TEST_F(ConnectionUT, ParseIpQuotation)
{
    const char *str = "{\"127.0.0.1\",127.0.0.2}";
    auto stringArray = parse_text_array_direct(str);
    EXPECT_EQ(stringArray.count, 2);
    EXPECT_EQ(strcmp(stringArray.elements[0], "127.0.0.1"), 0);
    EXPECT_EQ(strcmp(stringArray.elements[1], "127.0.0.2"), 0);
}

TEST_F(ConnectionUT, ParseIpEscape)
{
    const char *str = "{\\127.0.0.1,127.0.0.2}";
    auto stringArray = parse_text_array_direct(str);
    EXPECT_EQ(stringArray.count, 2);
    EXPECT_EQ(strcmp(stringArray.elements[0], "\\127.0.0.1"), 0);
    EXPECT_EQ(strcmp(stringArray.elements[1], "127.0.0.2"), 0);
}

TEST_F(ConnectionUT, ParsePort)
{
    const char *str = "{50069,50039}";
    auto stringArray = parse_text_array_direct(str);
    EXPECT_EQ(stringArray.count, 2);
    EXPECT_EQ(strcmp(stringArray.elements[0], "50069"), 0);
    EXPECT_EQ(strcmp(stringArray.elements[1], "50039"), 0);
}

TEST_F(ConnectionUT, ParseId)
{
    const char *str = "{0,1}";
    auto stringArray = parse_text_array_direct(str);
    EXPECT_EQ(stringArray.count, 2);
    EXPECT_EQ(strcmp(stringArray.elements[0], "0"), 0);
    EXPECT_EQ(strcmp(stringArray.elements[1], "1"), 0);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
