#pragma once

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <string_view>

#include "../common.h"
#include "coding.h"
#include "eloq_store.h"

constexpr char test_path[] = "/tmp/eloqstore";
static constexpr kvstore::TableIdent test_tbl_id = {"t0", 0};
const kvstore::KvOptions mem_store_opts = {};
const kvstore::KvOptions default_opts = {
    .db_path = test_path,
};

kvstore::EloqStore *InitStore(const kvstore::KvOptions &opts);

inline std::string_view ConvertIntKey(char *ptr, uint64_t key)
{
    uint64_t big_endian = kvstore::ToBigEndian(key);
    kvstore::EncodeFixed64(ptr, big_endian);
    return {ptr, sizeof(uint64_t)};
}

inline uint64_t ConvertIntKey(std::string_view key)
{
    uint64_t big_endian = kvstore::DecodeFixed64(key.data());
    return __builtin_bswap64(big_endian);
}
