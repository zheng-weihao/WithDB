#pragma once

#include "definitions.hpp"
#include "bptree.hpp"

namespace db {
	template<class T>
	struct IndexGuard { // 简单的包装bptree
		bptree<T>* tr;
		address iroot; // 通过读取iroot获取address位置

		IndexGuard(Keeper* k) { // 新建树
			tr = new bptree<T>(k);
			iroot = tr->pointroot;
		}

		IndexGuard(Keeper* k, address r) { // 载入已有的内容
			tr = new bptree<T>(k, r);
		}

		address fetch(T key) {
			return tr->search(key);
		}

		void allocate(T key, address value) {
			tr->insert(key, value);
		}

		void free(T key) {
			tr->delkey(key);
		}

		void reallocate(T key, address value) {
			tr->delkey(key);
			tr->insert(key, value);
		}

		void print(int i) {
			tr->print_tree(i);
		}

		~IndexGuard() {
			tr->close();
		}
		
	};


}