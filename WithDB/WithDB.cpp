// WithDB.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include "pch.h"

#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <vector>

using namespace std;

const char * path = "E:\\test.db";
std::random_device rd;
std::mt19937 random_engine(rd());

void init();
void testEndian();
void testCache();
void testPage();
void testRelation();
void testDriveBuffer();
void testDrive();
void testTranslator();
void testKeeper();

int main()
{
	init();
	// testEndian();
	// testCache();
	// testPage();
	testRelation();
	// testDriveBuffer();
	// testDrive();
	// testTranslator();
	// testKeeper();
	// test1();
	// test2();
	// test3();
	// test4();
	// test5();
	// test6();
	// test7();
	
	return 0;
}

#include <unordered_set>

void init() {
	cout << hex;
	ios::sync_with_stdio(false);
}

void testEndian() {
	for (double i = 0.01234; i < 0.01234  + 0.7; i += 0.1) {
		cout << db::decode<double>(db::encode(i)) << endl;
	}
}

db::size_t testHash(int addr) {
	return static_cast<db::size_t>(addr);
}

void testCache() {
	struct TestHandler : db::BasicCacheHandler<int, string> {
		bool onInsert(int addr, string &value) {
			value = to_string(addr + 1);
			return true;
		}

		bool onHit(int addr, string &value) {
			cout << "hit " << addr << endl;
			return true;
		}

		bool onErase(int addr, string &value) {
			cout << "kick " << addr << endl;
			return true;
		}
	};
	{
		
		TestHandler h;
		db::Cache<int, string, TestHandler, db::HashCacheCore<int, testHash>> c(h, 4);
		for (auto i = 8; i < 16; ++i) {
			cout << c.fetch(i) << endl;
		}
		for (auto i = 0; i < 4; ++i) {
			cout << c.fetch(i) << endl;
		}
		try {
			cout << c.fetch(4) << endl;
		} catch (std::runtime_error e) {
			cout << e.what() << endl;
		}
		for (auto i = 0; i < 8; ++i) {
			cout << c.fetch(i) << endl;
		}
		try {
			c.close();
		} catch (std::runtime_error e) {
			cout << e.what() << endl;
		}
	}
	{
		TestHandler h;
		db::Cache<int, string, TestHandler> c(h, 4);
		for (auto i = 8; i < 16; ++i) {
			cout << c.fetch(i) << endl;
		}
		for (auto i = 0; i < 4; ++i) {
			cout << c.fetch(i) << endl;
			c.pin(i);
		}
		try {
			cout << c.fetch(4) << endl;
		} catch (std::runtime_error e) {
			cout << e.what() << endl;
		}
		c.unpin(0);
		for (auto i = 0; i < 8; ++i) {
			cout << c.fetch(i) << endl;
		}
		try {
			c.close();
		} catch (std::runtime_error e) {
			cout << e.what() << endl;
		}
		for (auto i = 0; i < 8; ++i) {
			c.unpin(i);
		}
	}
}

void testPage() {
	{
		db::Container c;
		db::Page p(c, 0);
		for (int i = 0; i < 100; i += 4) {
			p.write(i, i);
		}
		for (int i = 0; i < 100; i += 4) {
			cout << p.read<int>(i) << endl;
		}
		string x = "123456789123456789123456789";
		p.write(x, 0);
		cout << p.read<string>(0) << endl;
	}
	struct TestHandler : db::BasicCacheHandler<int, db::Page> {
		bool onInsert(int addr, db::Page &p) {
			p.write(addr, 0);
			p.write("content = " + std::to_string(addr + 1), 4);
			return true;
		}
		bool onHit(int addr, db::Page &p) {
			cout << "hit " << addr << ' ' << p.read<string>(4) << endl;
			return true;
		}

		bool onErase(int addr, db::Page &p) {
			cout << "kick " << addr  << ' '<< p.read<string>(4) << endl;
			return true;
		}
	};
	{
		TestHandler h;
		db::Cache<int, db::Page, TestHandler> c(h, 4);
		for (auto i = 8; i < 16; ++i) {
			cout << c.fetch<db::Page>(i)->read<int>(0) << endl;
		}
		for (auto i = 0; i < 4; ++i) {
			cout << c.fetch<db::Page>(i)->read<int>(0) << endl;
			c.pin(i);
		}
		try {
			cout << c.fetch<db::Page>(5)->read<int>(0) << endl;
		} catch (std::runtime_error e) {
			cout << e.what() << endl;
		}
		c.unpin(0);
		for (auto i = 0; i < 8; ++i) {
			cout << c.fetch<db::Page>(i)->read<int>(0) << endl;
		}
		try {
			c.close();
		} catch (std::runtime_error e) {
			cout << e.what() << endl;
		}
		for (auto i = 0; i < 8; ++i) {
			c.unpin(i);
		}
	}
}

void testRelation() {
	db::Relation table;
	table._name = "foo";
	auto f = table.add(db::AttributeEntry("1", db::INT_T))
		.add(db::AttributeEntry("2", db::CHAR_T, 18))
		.add(db::AttributeEntry("3", db::VARCHAR_T, 100))
		.add(db::AttributeEntry("4", db::INT_T))
		.add(db::AttributeEntry("5", db::CHAR_T, 15))
		.add(db::AttributeEntry("6", db::DOUBLE_T))
		.add(db::AttributeEntry("7", db::VARCHAR_T, 50))
		.format();
	auto builder = table.builder();
	for (auto i = 0; i < 0x1; ++i) {
		db::TupleContainer c(table.maxTupleSize());
		db::Tuple tmp = builder.start(c)
			.build(1, 0)
			.build(string("Supplier#000000001"), 1)
			.build(string("N kD4on9OM Ipw3, gf0JBoQDd7tgrzrddZ"), 2)
			.build(17, 3)
			.build(string("27-918-335-1736"), 4)
			.build(5755.94, 5)
			.build(string("each slyly above the careful"), 6)
			.result();

		cout << table.read<int>(tmp, "0") << endl;
		cout << table.read<string>(tmp, 1) << endl;
		cout << table.read<string>(tmp, 2) << endl;
		cout << table.read<int>(tmp, 3) << endl;
		cout << table.read<string>(tmp, 4) << endl;
		cout << table.read<double>(tmp, 5) << endl;
		cout << table.read<string>(tmp, 6) << endl;
	}
}

// for temporary use to save strange file
void testDriveBuffer() {
	db::Container c(db::PAGE_SIZE);
	db::Page p(c, 0);
	{
		db::DriveBuffer buf(path, true);
		cout << buf.isOpen() << endl;
		cout << buf.size() << endl;
		buf.expand(db::PAGE_SIZE * 10);
		for (auto i = 0; i < 10; ++i) {
			p.write(0.123456 + 0.1 * i, 0);
			buf.put(p, i * db::PAGE_SIZE);
		}
		for (auto i = 9; i >= 0; --i) {
			buf.get(p, i * db::PAGE_SIZE);
			cout << p.read<double>(0) << endl;
		}
	}
	{
		db::DriveBuffer buf(path);
		cout << buf.size() << endl;
		for (auto i = 9; i >= 0; --i) {
			buf.get(p, i * db::PAGE_SIZE);
			cout << p.read<double>(0) << endl;
		}
	}
}

void testDrive() {
	unordered_set<db::drive_address> addrs;
	{
		db::Drive ctrl(path, true);
		for (int i = 0x1000; i > 0; --i) {
			auto ret = ctrl.allocate(i * db::PAGE_SIZE);
			// cout << ret << endl;
			if (ret % db::PAGE_SIZE) {
				cout << ret << endl;
			}
			if (addrs.find(ret) != addrs.end()) {
				throw runtime_error("testDrive");
			}
			addrs.insert(ret);
		}
		cout << addrs.size() << endl;
		for (auto addr : addrs) {
			ctrl.free(addr);
		}
		unordered_set<db::drive_address> s;
		auto t = ctrl._entry._userMasters.back();
		size_t cnt = 0;
		while (t) {
			if (s.find(t) != s.end()) {
				throw runtime_error("testDrive");
			}
			s.insert(t);
			auto ptr = ctrl._user._cache.fetch<db::DriveMasterPage>(t);
			if (t <= ptr->_forward) {
				throw runtime_error("testDrive");
			}
			t = ptr->_forward;
			cnt += ptr->_slaves.size();
		}
		cout << s.size() + cnt << endl;
		addrs.clear();
		for (int i = 0x100; i > 0; --i) {
			auto ret = ctrl.allocate(i * db::PAGE_SIZE);
			if (ret % db::PAGE_SIZE) {
				cout << ret << endl;
			}
			if (addrs.find(ret) != addrs.end()) {
				throw runtime_error("testDrive");
			}
			addrs.insert(ret);
		}
	}
	{
		db::Drive ctrl(path, false);
		for (auto addr : addrs) {
			ctrl.free(addr);
		}
		unordered_set<db::drive_address> s;
		auto t = ctrl._entry._userMasters.back();
		size_t cnt = 0;
		while (t) {
			if (s.find(t) != s.end()) {
				throw runtime_error("testDrive");
			}
			s.insert(t);
			auto ptr = ctrl._user._cache.fetch<db::DriveMasterPage>(t);
			if (t <= ptr->_forward) {
				throw runtime_error("testDrive");
			}
			t = ptr->_forward;
			cnt += ptr->_slaves.size();
		}
	}
	
}

void testTranslator() {
	size_t data_segment = db::Translator::segmentBegin(db::DATA_SEG) * db::SEGMENT_SIZE;
	unordered_map<db::address, db::drive_address> record;

	{
		db::Drive drive(path, true);
		db::Translator translator(drive);
		translator._params[0].first = 1;
		for (int i = 0; i < 0x100; ++i) {
			auto x = data_segment + i * db::PAGE_SIZE;
			auto y = drive.allocate();
			translator.link(x, y);
			record[x] = y;
		}
		for (int i = 0; i < 0x1000; ++i) {
			std::uniform_int_distribution<> dis(0, 0xff);
			auto x = data_segment + dis(random_engine) * db::PAGE_SIZE;
			if (record[x] != translator(x)) {
				throw runtime_error("testTranslator");
			}
		}
		for (int i = 0; i < 0x100; ++i) {
			auto x = data_segment + i * db::PAGE_SIZE;
			translator.unlink(x);
		}
		for (int i = 0; i < 0x100; ++i) {
			auto x = data_segment + i * db::PAGE_SIZE;
			bool flag;
			translator(x, flag);
			if (flag) {
				throw runtime_error("testTranslator");
			}
		} // TODO: relink
		for (int i = 0; i < 0x1000; ++i) {
			auto x = data_segment + i * db::PAGE_SIZE;
			auto y = drive.allocate();
			translator.link(x, y);
			record[x] = y;
		}
	}
	{
		db::Drive drive(path, false);
		db::Translator translator(drive);
		auto t = db::current();
		for (int i = 0; i < 0x100; ++i) {
			for (int j = 0; j < 0x1000; ++j) {
				auto x = data_segment + j * db::PAGE_SIZE;
				if (record[x] != translator(x)) {
					throw runtime_error("testTranslator");
				}
			}
		}
		cout << db::current() - t << endl;
	}
}

void testKeeper() {
	size_t data_segment = db::Translator::segmentBegin(db::DATA_SEG) * db::SEGMENT_SIZE;

	{
		db::Keeper keeper(path, true);
		keeper.start();
		keeper._translator._params[0].first = 1;
		for (int i = 0; i < 0x100; ++i) {
			auto x = data_segment + i * db::PAGE_SIZE;
			auto y = keeper.hold<db::VirtualPage>(x);
			y->write(-i, 0);
		}
		for (int i = 0; i < 0x1000; ++i) {
			std::uniform_int_distribution<> dis(0, 0xff);
			int j = dis(random_engine);
			auto x = data_segment + j * db::PAGE_SIZE;
			auto y = keeper.hold<db::VirtualPage>(x)->read<int>(0);
			if (y != -j) {
				throw runtime_error("testKeeper");
			}
		}
		for (int i = 0; i < 0x100; ++i) {
			auto x = data_segment + i * db::PAGE_SIZE;
			keeper.loosen(x);
		}
		for (int i = 0; i < 0x100; ++i) {
			auto x = data_segment + i * db::PAGE_SIZE;
			if (keeper.hold<db::VirtualPage>(x)->read<int>(0) != 0) {
				throw runtime_error("testKeeper");
			}
		} // TODO: relink
		for (int i = 0; i < 0x1000; ++i) {
			auto x = data_segment + i * db::PAGE_SIZE;
			auto y = keeper.hold<db::VirtualPage>(x);
			y->write(-i, 0);
		}
	}
	{
		db::Keeper keeper(path, false);
		keeper.start();
		auto t = db::current();
		for (int i = 0; i < 0x100; ++i) {
			for (int j = 0; j < 0x1000; ++j) {
				auto x = data_segment + j * db::PAGE_SIZE;
				if (keeper.hold<db::VirtualPage>(x)->read<int>(0) != -j) {
					throw runtime_error("testKeeper");
				}
			}
		}
		cout << db::current() - t << endl;
	}
}
//
//void test6() {
//	db::keeper k(path, false);
//	k.start();
//	cout << hex;
//	db::TuplePage x(std::move(k.hold(0)));
//	cout << x.addr << endl;
//	cout << x.get_free_space() << endl;
//	auto i = x.allocate(600);
//	cout << i << endl;
//	auto p = x.get(i);
//	cout << p.first << "  " << p.second << endl;
//	char s[] = "1234567890", r[20] = {};
//	x.copy_from(s, p.first, p.first + 10);
//	x.copy_to(r, p.first, p.first + 10);
//	cout << r << endl;
//	k.stop();
//	k.close();
//}
//
//void test7() {
//	db::AttributeTable table{
//		db::AttributeEntry(db::INT_T),
//		db::AttributeEntry(db::CHAR_T, 18),
//		db::AttributeEntry(db::VARCHAR_T),
//		db::AttributeEntry(db::INT_T),
//		db::AttributeEntry(db::CHAR_T, 15),
//		db::AttributeEntry(db::DOUBLE_T),
//		db::AttributeEntry(db::VARCHAR_T),
//	};
//	db::TupleBuilder builder;
//	builder.set_table(make_shared<db::AttributeTable>(table));
//	db::keeper k(path, true);
//	k.start();
//	for (auto i = 0; i < 0x100; ++i) {
//		builder.start();
//		builder.set(1, 0);
//		builder.set(string("Supplier#000000001"), 1);
//		builder.set(string("N kD4on9OM Ipw3, gf0JBoQDd7tgrzrddZ"), 2);
//		builder.set(17, 3);
//		builder.set(string("27-918-335-1736"), 4);
//		builder.set(5755.94, 5);
//		builder.set(string("each slyly above the careful"), 6);
//		auto out = builder.get();
//		builder.reset();
//		for (db::address addr = 0; addr >= 0; addr += db::PAGE_SIZE) {
//			db::TuplePage p(std::move(k.hold(addr)));
//			try {
//				p.load();
//			} catch (runtime_error e) {
//				p.init();
//			}
//			try {
//				auto result = p.allocate(static_cast<db::page_address>(out->size()));
//				auto pa = p.get(result);
//				p.copy_from(out->begin(), pa.first, pa.second);
//				p.dump();
//				cout << i << " -- " << p.addr << ":" << result << endl;
//				break;
//			} catch (runtime_error e) {
//				// cout << e.what() << endl;
//			}
//		}
//	}
//	int t = 0;
//	for (db::address addr = 0; addr >= 0; addr += db::PAGE_SIZE) {
//		db::TuplePage p(std::move(k.hold(addr)));
//		try {
//			p.load();
//		} catch (runtime_error e) {
//			break;
//		}
//		for (auto &entry : p._entries) {
//			auto pa = p.get(entry.index);
//			if (pa.second == 0) {
//				continue;
//			}
//			db::tuple tmp(pa.second - pa.first);
//			p.copy_to(tmp.begin(), pa.first, pa.second);
//			cout << table.get<int>(tmp, 0) << endl;
//			cout << table.get<string>(tmp, 1) << endl;
//			cout << table.get<string>(tmp, 2) << endl;
//			cout << table.get<int>(tmp, 3) << endl;
//			cout << table.get<string>(tmp, 4) << endl;
//			cout << table.get<double>(tmp, 5) << endl;
//			cout << table.get<string>(tmp, 6) << endl;
//			cout << t++ << " -- " << p.addr << ":" << entry.index << endl;
//			p.free(entry.index);
//		}
//	}
//	for (auto i = 0; i < 0x100; ++i) {
//		builder.start();
//		builder.set(1, 0);
//		builder.set(string("Supplier#000000001"), 1);
//		builder.set(string("N kD4on9OM Ipw3, gf0JBoQDd7tgrzrddZ"), 2);
//		builder.set(17, 3);
//		builder.set(string("27-918-335-1736"), 4);
//		builder.set(5755.94, 5);
//		builder.set(string("each slyly above the careful"), 6);
//		auto out = builder.get();
//		builder.reset();
//		for (db::address addr = 0; addr >= 0; addr += db::PAGE_SIZE) {
//			db::TuplePage p(std::move(k.hold(addr)));
//			try {
//				p.load();
//			} catch (runtime_error e) {
//				p.init();
//			}
//			try {
//				auto result = p.allocate(static_cast<db::page_address>(out->size()));
//				auto pa = p.get(result);
//				p.copy_from(out->begin(), pa.first, pa.second);
//				cout << i << " -- " << p.addr << ":" << result << endl;
//				break;
//			} catch (runtime_error e) {
//				// cout << e.what() << endl;
//			}
//		}
//	}
//	for (auto i = 0; i < 0x100; ++i) {
//
//		builder.start();
//		builder.set(1, 0);
//		builder.set(string("Supplier#000000001"), 1);
//		builder.set(string("N kD4on9OM Ipw3, gf0JBoQDd7tgrzrddZ"), 2);
//		builder.set(17, 3);
//		builder.set(string("27-918-335-1736"), 4);
//		builder.set(5755.94, 5);
//		builder.set(string("each slyly above the careful"), 6);
//		auto out = builder.get();
//		builder.reset();
//		for (db::address addr = 0; addr >= 0; addr += db::PAGE_SIZE) {
//			db::TuplePage p(std::move(k.hold(addr)));
//			try {
//				p.load();
//			} catch (runtime_error e) {
//				p.init();
//			}
//			try {
//				auto result = p.allocate(static_cast<db::page_address>(out->size()), false);
//				auto pa = p.get(result);
//				p.copy_from(out->begin(), pa.first, pa.second);
//				cout << i << " -- " << p.addr << ":" << result << endl;
//				break;
//			} catch (runtime_error e) {
//				cout << e.what() << endl;
//			}
//		}
//	}
//
//	k.stop();
//	k.close();
//}



// Run program: Ctrl + F5 or Debug > Start Without Debugging menu
// Debug program: F5 or Debug > Start Debugging menu

// Tips for Getting Started: 
//   1. Use the Solution Explorer window to add/manage files
//   2. Use the Team Explorer window to connect to source control
//   3. Use the Output window to see build output and other messages
//   4. Use the Error List window to view errors
//   5. Go to Project > Add New Item to create new code files, or Project > Add Existing Item to add existing code files to the project
//   6. In the future, to open this project again, go to File > Open > Project and select the .sln file
