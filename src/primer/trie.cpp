#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  throw NotImplementedException("Trie::Get is not implemented.");
  // 1、空树
  if (!root_) {
    return nullptr;
  }
  auto ptr = root_;

  for (auto c : key) {
    if (ptr->children_.count(c) != 0) {
      // 存在
      ptr = ptr->children_.at(c);
    } else {
      return nullptr;
    }
  }
  // 终止节点
  if (!ptr->is_value_node_) {
    return nullptr;
  }
  // 类型匹配吗？
  auto value = dynamic_cast<const TrieNodeWithValue<T> *>(ptr.get());
  if (value != nullptr) {
    return value->value_.get();
  }
  return nullptr;
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // 1.空树
  if (!root_) {
    auto trienode = std::make_shared<TrieNode>();
    return Trie(trienode).Put(key, std::move(value));
  }

  // 2.空串
  std::shared_ptr<TrieNode> root = root_->Clone();
  auto ptr = root;
  if (key.empty()) {
    ptr = std::make_shared<TrieNodeWithValue<T>>(ptr->children_, std::make_shared<T>(std::move(value)));
    return Trie(ptr);
  }

  size_t i = 0;
  // 最后一个字符特殊处理
  for (; i < key.size() - 1; i++) {
    if (ptr->children_.count(key[i]) == 0) {
      // 不存在这个分支
      std::shared_ptr<TrieNode> node = std::make_shared<TrieNode>();
      ptr->children_[key[i]] = node;
      ptr = node;
    } else {
      // 存在这个分支
      std::shared_ptr<TrieNode> new_node = ptr->children_.at(key[i])->Clone();
      ptr->children_[key[i]] = new_node;
      ptr = new_node;
    }
  }
  // 只剩最后一个字符没有处理
  std::shared_ptr<TrieNode> vale_node;
  if (ptr->children_.count(key.back()) == 0) {
    // 不存在
    vale_node = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  } else {
    // 存在
    vale_node = std::make_shared<TrieNodeWithValue<T>>(ptr->children_[key.back()]->children_,
                                                       std::make_shared<T>(std::move(value)));
  }
  ptr->children_[key.back()] = vale_node;
  return Trie(root);

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

std::shared_ptr<const TrieNode> Trie::Dfs(const std::shared_ptr<const TrieNode> &root, std::string_view key,
                                          size_t index) const {
  if (index == key.size()) {
    if (root->children_.empty()) {
      return nullptr;
    }
    return std::make_shared<const TrieNode>(root->children_);
  }
  std::shared_ptr<const TrieNode> new_node;
  auto t = std::const_pointer_cast<TrieNode>(root);
  if (t->children_.find(key[index]) != root->children_.end()) {
    new_node = Dfs(t->children_[key[index]], key, index + 1);
    auto node = root->Clone();
    if (new_node) {
      node->children_[key[index]] = new_node;
    } else {
      node->children_.erase(key[index]);
      if (!node->is_value_node_ && node->children_.empty()) {
        return nullptr;
      }
    }
    return node;
  }
  return root;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  auto root = Dfs(root_, key, 0);
  return Trie(root);

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
