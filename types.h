#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <string>

#include "coding.h"

namespace kvstore
{
using PageId = uint32_t;
constexpr PageId MaxPageId = UINT32_MAX;

using FilePageId = uint64_t;
constexpr FilePageId MaxFilePageId = UINT64_MAX;

using FileId = uint64_t;
static constexpr FileId MaxFileId = UINT64_MAX;

constexpr char FileNameSeparator = '_';
static constexpr char FileNameData[] = "data";
static constexpr char FileNameManifest[] = "manifest";
static constexpr char FileNameTmpfile[] = "tmpfile";

struct TableIdent
{
    static constexpr char Separator = '-';
    friend bool operator==(const TableIdent &lhs, const TableIdent &rhs)
    {
        return lhs.tbl_name_ == rhs.tbl_name_ &&
               lhs.partition_id_ == rhs.partition_id_;
    }
    friend std::ostream &operator<<(std::ostream &os, const TableIdent &point);

    std::string ToString() const;
    static TableIdent FromString(const std::string &str);
    void SerializeTo(std::string &dst) const;
    size_t Hash() const;
    bool IsValid() const;

    std::string tbl_name_;
    uint32_t partition_id_;
};

inline std::string TableIdent::ToString() const
{
    return tbl_name_ + Separator + std::to_string(partition_id_);
}

inline TableIdent TableIdent::FromString(const std::string &str)
{
    size_t p = str.find(Separator);
    if (p == std::string::npos)
    {
        return {};
    }

    try
    {
        uint32_t num = std::stoul(str.data() + p + 1);
        return {str.substr(0, p), num};
    }
    catch (...)
    {
        return {};
    }
}

inline void TableIdent::SerializeTo(std::string &dst) const
{
    PutLengthPrefixedSlice(&dst, tbl_name_);
    PutVarint32(&dst, partition_id_);
}

inline size_t TableIdent::Hash() const
{
    return std::hash<std::string>()(tbl_name_) * 23 + partition_id_;
}

inline bool TableIdent::IsValid() const
{
    return !tbl_name_.empty();
}

inline std::ostream &operator<<(std::ostream &out, const TableIdent &tid)
{
    out << tid.tbl_name_ << TableIdent::Separator << tid.partition_id_;
    return out;
}

}  // namespace kvstore

template <>
struct std::hash<kvstore::TableIdent>
{
    std::size_t operator()(const kvstore::TableIdent &tbl_ident) const
    {
        return tbl_ident.Hash();
    }
};