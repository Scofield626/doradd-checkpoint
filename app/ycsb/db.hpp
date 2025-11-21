#pragma once

#include "constants.hpp"

#include <cpp/when.h>

using namespace verona::rt;
using namespace verona::cpp;

template<typename T>
struct Index
{
private:
  std::array<cown_ptr<T>, DB_SIZE> map;
  uint64_t cnt = 0;

public:
  Index() : map(std::array<cown_ptr<T>, DB_SIZE>()) {}

  cown_ptr<T>* get_row_addr(uint64_t key)
  {
    return &map[key];
  }

  cown_ptr<T>&& get_row(uint64_t key)
  {
    return std::move(map[key]);
  }

  uint64_t insert_row(cown_ptr<T> r)
  {
    if (cnt < DB_SIZE)
    {
      map[cnt] = r;
      return cnt++;
    }

    throw std::out_of_range("Index is full");
  }
};
