/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "utils/utils_standalone.h"

#include <stdbool.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/time.h>

int64_t GetCurrentTimeInUs()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000 + tv.tv_usec;
}

int64_t StringToInt64(const char *data) { return strtoll(data, NULL, 10); }
uint64_t StringToUint64(const char *data) { return strtoull(data, NULL, 10); }
int32_t StringToInt32(const char *data) { return strtol(data, NULL, 10); }
uint32_t StringToUint32(const char *data) { return strtoul(data, NULL, 10); }

int pathcmp(const char *p1, const char *p2)
{
    const unsigned char *s1 = (const unsigned char *)p1;
    const unsigned char *s2 = (const unsigned char *)p2;
    unsigned char c1, c2;
    do {
        c1 = (unsigned char)*s1++;
        c2 = (unsigned char)*s2++;
        if (c1 == '\0')
            return c1 - c2;
    } while (c1 == c2);

    if (c2 == '\0' || c2 == '/')
        return 1;
    if (c1 == '/')
        return -1;
    return c1 - c2;
}

/*
 * Parse string in format {ip1, ip2...} / {port1, port2...} / {id1, id2...}...
*/
StringArray parse_text_array_direct(const char *array_str) {
    StringArray result = {NULL, 0};

    if (!array_str || array_str[0] != '{') {
        printf("parse_text_array_direct(): invalid param array_str");
        return result;
    }

    char *str = strdup(array_str);
    if (!str) {
        printf("parse_text_array_direct(): strdup failed");
        return result;
    }

    // remove {}
    int len = strlen(str);
    if (len > 0 && str[len-1] == '}') {
        str[len-1] = '\0';
    }
    if (str[0] == '{') {
        char *content = str + 1;
        // number of elements' strings
        int max_elements = 1;
        for (char *p = content; *p; p++) {
            if (*p == ',') max_elements++;
        }
        result.elements = malloc(max_elements * sizeof(char*));
        if (!result.elements) {
            printf("parse_text_array_direct(): malloc result.elements failed");
            free(str);
            return result;
        }

        // start of an element
        char *start = content;
        // parse by ',', consider "element" and \\char, \\ accounts for only one charater
        bool in_quotes = false, escaped = false;

        // may have \\0, so unable to stop at \0
        for (char *p = content; ; p++) {
            if (*p == '"' && !escaped) {
                in_quotes = !in_quotes;
            } else if (*p == '\\' && !escaped) {
                // skip processing next char
                escaped = true;
            } else if ((*p == ',' || *p == '\0') && !in_quotes && !escaped) {
                // end of an element
                int elem_len = p - start;
                if (elem_len > 0 && !(elem_len == 2 && *start == '"' && *(p - 1) == '"')) {
                    // possibly embraced by ""
                    if (elem_len > 2 && *start == '"' && *(p - 1) == '"') {
                        result.elements[result.count] = strndup(start + 1, elem_len - 2);
                    } else {
                        result.elements[result.count] = strndup(start, elem_len);
                    }
                    if (!result.elements[result.count]) {
                        printf("parse_text_array_direct(): strndup result.elements[] failed");
                        free_string_array(&result);
                        free(str);
                        return result;
                    }
                    result.count++;
                } else {
                    printf("parse_text_array_direct(): empty elment to parse!");
                    free_string_array(&result);
                    free(str);
                    return result;
                }

                start = p + 1; // next element

                if (*p == '\0') break;
            } else {
                escaped = false;
                continue;
            }
        }
    }

    free(str);
    return result;
}

void free_string_array(StringArray *arr) {
    for (int i = 0; i < arr->count; i++) {
        free(arr->elements[i]);
    }
    free(arr->elements);
    arr->elements = NULL;
    arr->count = 0;
}