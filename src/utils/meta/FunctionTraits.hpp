#ifndef PROJECT_UTILITY_META_FUNCTION_TRAITS_HPP_
#define PROJECT_UTILITY_META_FUNCTION_TRAITS_HPP_

#include <tuple>

namespace meta {

template<typename F>
struct FunctionTraits;

namespace internal {

template<typename T>
struct FunctionTraits_0;

template<typename ClassT, typename ReturnT, typename ...Args>
struct FunctionTraits_0<ReturnT(ClassT::*)(Args...) const> {
  static constexpr std::size_t arity = sizeof...(Args);

  using return_type = ReturnT;

  template<std::size_t i>
  using argument_type = typename std::tuple_element<i, std::tuple<Args...>>::type;
};

}  // namespace internal

template<typename F>
struct FunctionTraits : internal::FunctionTraits_0<decltype(&F::operator())> {
};

}  // namespace meta

#endif  // PROJECT_UTILITY_META_FUNCTION_TRAITS_HPP_
