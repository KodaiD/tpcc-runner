#pragma once

#include <map>

#include "b_tree/component/oml/b_tree.hpp"
#include "protocols/common/schema.hpp"

using namespace dbgroup::index::b_tree::component::oml;
using namespace dbgroup::index::b_tree::component;

template <typename Value_>
class BTreeIndexes {
public:
    using Key = uint64_t;
    using Value = Value_;
    using BT = BTree<Key, Value*, std::less<Key>, false>;
    using Node = typename BT::Node_t;
    using NodeMap = std::unordered_map<Node*, uint64_t>;
    using NodeInfo = std::tuple<Node*, uint64_t, uint64_t>;

    enum Result {
        OK = 0,
        NOT_FOUND,
        BAD_INSERT,
        NOT_INSERTED,
        NOT_DELETED,
        BAD_SCAN,
    };

    Result find(TableID table_id, Key key, Value*& val) {
        NodeMap nm;
        auto& bt = indexes[table_id];
        auto result = bt.OptimisticRead(key, val, nm);
        if (result == NodeRC::kKeyAlreadyInserted) {
            return OK;
        } else {
            return NOT_FOUND;
        }
    }

    Result find(TableID table_id, Key key, Value*& val, NodeMap& nm) {
        auto& bt = indexes[table_id];
        auto result = bt.OptimisticRead(key, val, nm);
        if (result == NodeRC::kKeyAlreadyInserted) {
            return OK;
        } else {
            return NOT_FOUND;
        }
    }

    Result insert(TableID table_id, Key key, Value* val) {
        auto& bt = indexes[table_id];
        NodeInfo ni;
        auto result = bt.TryInsert(key, val, ni);
        if (result == NodeRC::kCompleted) {
            return OK;
        } else {
            return NOT_INSERTED;
        }
    }

    Result insert(TableID table_id, Key key, Value* val, NodeMap& nm) {
        auto& bt = indexes[table_id];
        NodeInfo ni;
        auto result = bt.TryInsert(key, val, ni);
        if (result == NodeRC::kCompleted) {
            if (auto itr = nm.find(std::get<0>(ni)); itr == nm.end()) {
                return OK;
            } else if (itr->second != std::get<1>(ni)) {
                return BAD_INSERT;
            } else {
                itr->second = std::get<2>(ni);
                return OK;
            }
        } else {
            return NOT_INSERTED;
        }
    }

    // Result get_next_kv(TableID table_id, Key key, Key& next_key, Value*& next_val) {
    //     // TODO:
    //     return NOT_FOUND;
    // }

    // template <typename NodeFunc, typename KVFunc>
    // Result get_kv_in_range(
    //     TableID table_id, Key lkey, Key rkey, NodeFunc&& per_node_func, KVFunc&& per_kv_func) {
    //     // TODO:
    //     return NOT_FOUND;
    // }

    // Result get_kv_in_range(
    //     TableID table_id, Key lkey, Key rkey, int64_t count, std::map<Key, Value*>& kv_map) {
    //     // TODO:
    //     return NOT_FOUND;
    // }

    Result get_kv_in_range(
        TableID table_id, Key lkey, Key rkey, [[maybe_unused]] int64_t count,
        std::map<Key, Value*>& kv_map, NodeMap& nm) {
        NodeMap tmp_nm;
        auto& bt = indexes[table_id];
        bt.OptimisticScan(lkey, rkey, kv_map, tmp_nm);
        for (auto& [node, version]: tmp_nm) {
            if (auto itr = nm.find(node); itr == nm.end()) {
                nm.emplace_hint(itr, node, version);
            } else if (itr->second != version) {
                return BAD_SCAN;
            }
        }
        return OK;
    }

    // template <typename NodeFunc, typename KVFunc>
    // Result get_kv_in_rev_range(
    //     TableID table_id, Key lkey, Key rkey, NodeFunc&& per_node_func, KVFunc&& per_kv_func) {
    //     // TODO:
    //     return NOT_FOUND;
    // }

    // Result get_kv_in_rev_range(
    //     TableID table_id, Key lkey, Key rkey, int64_t count, std::map<Key, Value*>& kv_map) {
    //     // TODO:
    //     return NOT_FOUND;
    // }

    Result get_kv_in_rev_range(
        TableID table_id, Key lkey, Key rkey, [[maybe_unused]] int64_t count,
        std::map<Key, Value*>& kv_map, NodeMap& nm) {
        NodeMap tmp_nm;
        auto& bt = indexes[table_id];
        bt.OptimisticScan(lkey, rkey, kv_map, tmp_nm);
        for (auto& [node, version]: tmp_nm) {
            if (auto itr = nm.find(node); itr == nm.end()) {
                nm.emplace_hint(itr, node, version);
            } else if (itr->second != version) {
                return BAD_SCAN;
            }
        }
        return OK;
    }

    // Result remove(TableID table_id, Key key) { return NOT_FOUND; }

    uint64_t get_version_value([[maybe_unused]] TableID table_id, Node* node) {
        return node->GetVersion();
    }

    static BTreeIndexes<Value>& get_index() {
        static BTreeIndexes<Value> idx;
        return idx;
    }

private:
    std::unordered_map<TableID, BT> indexes;
};