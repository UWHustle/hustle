#ifndef PROJECT_UTILITY_META_MULTIPLE_DISPATCHER_HPP_
#define PROJECT_UTILITY_META_MULTIPLE_DISPATCHER_HPP_

#include <type_traits>

#include "utils/meta/Common.hpp"
#include "utils/meta/TypeList.hpp"

#include "glog/logging.h"

namespace meta {

template <typename S, typename ...Ts>
struct MultipleDispatcher;

struct TrivialDispatcher;

namespace internal {

template <typename PredicateTL, typename ResultTL, typename Enable = void>
struct MultipleDispatcher_2;

template <typename PredicateTL, typename ResultTL>
struct MultipleDispatcher_2<PredicateTL, ResultTL,
                            std::enable_if_t<PredicateTL::length == 0>> {
  template <typename Target, typename OnSuccess, typename OnFailure>
  static inline auto InvokeOnInner(const OnSuccess &success,
                                   const OnFailure &failure,
                                   std::enable_if_t<Target::kSuccess>* = 0) {
    return success(typename Target::type());
  }

  template <typename Target, typename OnSuccess, typename OnFailure>
  static inline auto InvokeOnInner(const OnSuccess &success,
                                   const OnFailure &failure,
                                   std::enable_if_t<!Target::kSuccess>* = 0) {
    return failure(typename Target::type());
  }

  template <typename Transformer, typename OnSuccess, typename OnFailure>
  static inline auto InvokeOn(const OnSuccess &success,
                              const OnFailure &failure) {
    using Target = typename Transformer::template transform<ResultTL>;
    return InvokeOnInner<Target>(success, failure);
  }
};

template <typename PredicateTL, typename ResultTL>
struct MultipleDispatcher_2<
    PredicateTL, ResultTL,
    std::enable_if_t<PredicateTL::length != 0 &&
                     PredicateTL::head::template apply<ResultTL>::value>>
    : MultipleDispatcher_2<typename PredicateTL::tail, ResultTL> {};

template <typename PredicateTL, typename ResultTL>
struct MultipleDispatcher_2<
    PredicateTL, ResultTL,
    std::enable_if_t<PredicateTL::length != 0 &&
                     !PredicateTL::head::template apply<ResultTL>::value>> {
  template <typename Transformer, typename OnSuccess, typename OnFailure>
  static inline auto InvokeOn(const OnSuccess &success,
                              const OnFailure &failure) {
    return failure(TypeList<>());
  }
};

template <typename PredicateTL, typename PrevTL,
          typename RestCtxTL, typename CurrentCtx, typename Enable = void>
struct MultipleDispatcher_1;

template <typename PredicateTL, typename PrevTL, typename RestCtxTL,
          typename VT, typename VL, typename TL,
          typename ResTL, typename ...FutureArgTypes>
struct MultipleDispatcher_1<
    PredicateTL, PrevTL, RestCtxTL, TypeList<
        VT, VL, TL, TypeList<FutureArgTypes...>, ResTL>,
    std::enable_if_t<VL::length == 0 && !std::is_same<VT, void>::value>> {
  template <typename Transformer, typename OnSuccess, typename OnFailure>
  static inline auto InvokeOn(const VT &value,
                              const FutureArgTypes &...rest_values,
                              const OnSuccess &success,
                              const OnFailure &failure) {
    return failure(TypeList<>());
  }
};

template <typename PredicateTL, typename PrevTL, typename RestCtxTL,
          typename VT, typename VL, typename TL,
          typename ResTL, typename ...FutureArgTypes>
struct MultipleDispatcher_1<
    PredicateTL, PrevTL, RestCtxTL, TypeList<
        VT, VL, TL, TypeList<FutureArgTypes...>, ResTL>,
    std::enable_if_t<VL::length == 0 &&
                     RestCtxTL::length != 0 &&
                     std::is_same<VT, void>::value>> {
  template <typename Transformer, typename OnSuccess, typename OnFailure>
  static inline auto InvokeOn(const FutureArgTypes &...rest_values,
                              const OnSuccess &success,
                              const OnFailure &failure) {
    using CurrentTL = typename PrevTL::template append<ResTL>;
    return MultipleDispatcher_1<PredicateTL, CurrentTL,
                                typename RestCtxTL::tail,
                                typename RestCtxTL::head>
        ::template InvokeOn<Transformer,
                            OnSuccess,
                            OnFailure>(rest_values..., success, failure);
  }
};

template <typename PredicateTL, typename PrevTL, typename RestCtxTL,
          typename VT, typename VL, typename TL,
          typename ResTL, typename ...FutureArgTypes>
struct MultipleDispatcher_1<
    PredicateTL, PrevTL, RestCtxTL, TypeList<
        VT, VL, TL, TypeList<FutureArgTypes...>, ResTL>,
    std::enable_if_t<VL::length == 0 &&
                     RestCtxTL::length == 0 &&
                     std::is_same<VT, void>::value>> {
  template <typename Transformer, typename OnSuccess, typename OnFailure>
  static inline auto InvokeOn(const OnSuccess &success,
                              const OnFailure &failure) {
      using ResultTL = typename PrevTL::template append<ResTL>;
      return MultipleDispatcher_2<PredicateTL, ResultTL>
          ::template InvokeOn<Transformer,
                              OnSuccess,
                              OnFailure>(success, failure);
  }
};

template <typename PredicateTL, typename PrevTL, typename RestCtxTL,
          typename VT, typename VL, typename TL,
          typename FutureArgTL, typename ResTL>
struct MultipleDispatcher_1<
    PredicateTL, PrevTL, RestCtxTL,
    TypeList<VT, VL, TL, FutureArgTL, ResTL>,
    std::enable_if_t<VL::length != 0 && RestCtxTL::length == 0>> {
  template <typename Transformer, typename OnSuccess, typename OnFailure>
  static inline auto InvokeOn(const VT &value,
                              const OnSuccess &success,
                              const OnFailure &failure) {
    if (value == VL::head::value) {
      using ResultTL = typename PrevTL::template push_back<typename TL::head>
                                      ::template append<ResTL>;
      return MultipleDispatcher_2<PredicateTL, ResultTL>
          ::template InvokeOn<Transformer,
                              OnSuccess,
                              OnFailure>(success, failure);
    } else {
      return MultipleDispatcher_1<PredicateTL, PrevTL, RestCtxTL,
                                  TypeList<VT, typename VL::tail, typename TL::tail,
                                           FutureArgTL, ResTL>>
          ::template InvokeOn<Transformer,
                              OnSuccess,
                              OnFailure>(value, success, failure);
    }
  }
};

template <typename PredicateTL, typename PrevTL, typename RestCtxTL,
          typename VT, typename VL, typename TL,
          typename ResTL, typename ...FutureArgTypes>
struct MultipleDispatcher_1<
    PredicateTL, PrevTL, RestCtxTL, TypeList<
        VT, VL, TL, TypeList<FutureArgTypes...>, ResTL>,
    std::enable_if_t<VL::length != 0 && RestCtxTL::length != 0>> {
  template <typename Transformer, typename OnSuccess, typename OnFailure>
  static inline auto InvokeOn(const VT &value,
                              const FutureArgTypes &...rest_values,
                              const OnSuccess &success,
                              const OnFailure &failure) {
    if (value == VL::head::value) {
      using CurrentTL = typename PrevTL::template push_back<typename TL::head>
                                       ::template append<ResTL>;
      return MultipleDispatcher_1<PredicateTL, CurrentTL,
                                  typename RestCtxTL::tail,
                                  typename RestCtxTL::head>
          ::template InvokeOn<Transformer,
                              OnSuccess,
                              OnFailure>(rest_values..., success, failure);
    } else {
      return MultipleDispatcher_1<PredicateTL, PrevTL, RestCtxTL,
                                  TypeList<VT, typename VL::tail, typename TL::tail,
                                           TypeList<FutureArgTypes...>, ResTL>>
          ::template InvokeOn<Transformer,
                              OnSuccess,
                              OnFailure>(value, rest_values..., success, failure);
    }
  }
};


struct IdentityTransformer {
  template <typename TL>
  struct transform {
    static constexpr bool kSuccess = true;
    using type = TL;
  };
};

template <typename PredicateTL, typename Transformer, typename EntryTL,
          typename CtxTL, typename FutureArgTL, typename Enable = void>
struct MultipleDispatcher_0;

template <typename PredicateTL, typename Transformer, typename EntryTL,
          typename CtxTL, typename ...ArgTypes>
struct MultipleDispatcher_0<
    PredicateTL, Transformer, EntryTL, CtxTL, TypeList<ArgTypes...>,
    std::enable_if_t<EntryTL::length == 0>> {
  using dispatcher = MultipleDispatcher_1<
      PredicateTL, TypeList<>, typename CtxTL::tail, typename CtxTL::head>;

  template <typename Functor>
  static inline auto InvokeOn(const ArgTypes &...args,
                              const Functor &functor) {
    const auto failure = [](auto emptylist)
        -> decltype(functor(TypeList<>())) {
      LOG(FATAL) << "Unexpected value for SequenceDispather::InvokeOn";
    };

    return dispatcher::template InvokeOn<
        Transformer, Functor, decltype(failure)>(
            args..., functor, failure);
  }

  static inline bool IsInvokable(const ArgTypes &...args) {
    const auto success = [](auto typelist) -> bool {
      return true;
    };
    const auto failure = [](auto emptylist) -> bool {
      return false;
    };

    return dispatcher::template InvokeOn<
        IdentityTransformer, decltype(success), decltype(failure)>(
            args..., success, failure);
  }
};

template <typename PredicateTL, typename Transformer, typename EntryTL,
          typename CtxTL, typename FutureArgTL>
struct MultipleDispatcher_0<
    PredicateTL, Transformer, EntryTL, CtxTL, FutureArgTL,
    std::enable_if_t<EntryTL::length != 0 &&
                     !std::is_same<typename EntryTL::head::head, void>::value>>
    : MultipleDispatcher_0<
          PredicateTL, Transformer, typename EntryTL::tail,
          typename CtxTL::template push_front<TypeList<
              typename EntryTL::head::template at<0>,
              typename EntryTL::head::template at<1>,
              typename EntryTL::head::template at<2>,
              FutureArgTL,
              typename EntryTL::head::template at<3>>>,
          typename FutureArgTL::template push_front<
              typename EntryTL::head::template at<0>>> {};

template <typename PredicateTL, typename Transformer, typename EntryTL,
          typename CtxTL, typename FutureArgTL>
struct MultipleDispatcher_0<
    PredicateTL, Transformer, EntryTL, CtxTL, FutureArgTL,
    std::enable_if_t<EntryTL::length != 0 &&
                     std::is_same<typename EntryTL::head::head, void>::value>>
    : MultipleDispatcher_0<
          PredicateTL, Transformer, typename EntryTL::tail,
          typename CtxTL::template push_front<TypeList<
              typename EntryTL::head::template at<0>,
              typename EntryTL::head::template at<1>,
              typename EntryTL::head::template at<2>,
              FutureArgTL,
              typename EntryTL::head::template at<3>>>,
          FutureArgTL> {};

template <typename Predicate, std::size_t start, std::size_t length>
struct PredicateDelegator {
  using predicate = Predicate;

  static constexpr std::size_t kStart = start;
  static constexpr std::size_t kLength = length;

  template <typename TL>
  using apply = typename Predicate::template apply<
      typename TL::template skip<start>::template take<length>>;
};

template <typename TL>
struct PredicateDelegatorShift {
  using PD = typename TL::head;
  static constexpr std::size_t offset = TL::tail::head::value;

  using type = PredicateDelegator<
      typename PD::predicate, PD::kStart + offset, PD::kLength>;
};

template <typename Entry>
struct ResultLengthForEntry {
  using type = std::integral_constant<
      std::size_t,
      Entry::template at<3>::length +
          !std::is_same<typename Entry::head, void>::value>;
};

// EntryTL: TypeList of TypeList<VT, VL, TL, ResTL, PL> in REVERSE order.
template <typename EntryTL, typename PredicateTL,
          typename Transformer = IdentityTransformer>
struct MultipleDispatcher
    : MultipleDispatcher_0<PredicateTL, Transformer, EntryTL,
                           TypeList<>, TypeList<>> {
  using entries = EntryTL;
  using predicates = PredicateTL;

  static constexpr std::size_t kOutputSize =
      entries::template map<ResultLengthForEntry>
             ::template foldl<Add,
                              std::integral_constant<std::size_t, 0>>::value;

  static constexpr bool kTrivial =
      entries::length == 1 && predicates::length == 0 &&
          std::is_same<typename entries::head::head, void>::value;

  template <typename OtherTF>
  using set_transformer = MultipleDispatcher<EntryTL, PredicateTL, OtherTF>;

  template <typename NextDispatcher>
  using set_next = MultipleDispatcher<
      typename NextDispatcher::entries::template append<entries>,
      typename predicates::template append<
          typename NextDispatcher::predicates
              ::template cartesian_product<
                    TypeList<std::integral_constant<std::size_t, kOutputSize>>>
              ::template map<PredicateDelegatorShift>>>;

  template <typename PrevDispatcher>
  using set_prev = MultipleDispatcher<
      typename entries::template append<typename PrevDispatcher::entries>,
      typename PrevDispatcher::predicates::template append<
          typename predicates
              ::template cartesian_product<
                    TypeList<std::integral_constant<
                        std::size_t, PrevDispatcher::kOutputSize>>>
              ::template map<PredicateDelegatorShift>>>;  // NOLINT[build/include_what_you_use]

  template <typename T>
  using push_back = MultipleDispatcher<
      typename EntryTL::tail::template push_front<TypeList<
          typename EntryTL::head::template at<0>,
          typename EntryTL::head::template at<1>,
          typename EntryTL::head::template at<2>,
          typename EntryTL::head::template at<3>::template push_back<T>>>,
      PredicateTL>;

  template <typename Predicate>
  using add_predicate = MultipleDispatcher<
      EntryTL,
      typename PredicateTL::template push_back<
          PredicateDelegator<Predicate, 0, kOutputSize>>>;
};

template <typename VL, typename TL, typename Enable = void>
struct MultipleDispatcherOverload {
  static_assert(TL::length <= 1,
                "Invalid template arguments for MultipleDispatcher");
  static_assert(VL::length > 0,
                "Sequence cannot be empty.");
};

template <typename VL, typename TL>
struct MultipleDispatcherOverload<
    VL, TL, std::enable_if_t<VL::length != 0 && TL::length == 0>>
    : public MultipleDispatcher<
          TypeList<TypeList<typename VL::head::value_type, VL, VL, TypeList<>>>,
          TypeList<>> {};

template <typename VL, typename TL>
struct MultipleDispatcherOverload<
    VL, TL, std::enable_if_t<VL::length != 0 && TL::length == 1>>
    : public MultipleDispatcher<
          TypeList<TypeList<typename VL::head::value_type, VL,
                            typename TL::head, TypeList<>>>,
          TypeList<>> {};

}  // namespace internal

struct TrivialDispatcher
    : internal::MultipleDispatcher<
          TypeList<TypeList<void, TypeList<>, TypeList<>, TypeList<>>>,
          TypeList<>> {};

template <typename S, typename ...Ts>
struct MultipleDispatcher
    : internal::MultipleDispatcherOverload<typename S::template bind_to<TypeList>,
                                           TypeList<Ts...>> {};

}  // namespace meta

#endif  // PROJECT_UTILITY_META_MULTIPLE_DISPATCHER_HPP_
