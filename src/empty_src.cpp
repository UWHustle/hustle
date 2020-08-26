// On OSX, ranlib complains if a static library archive contains no symbols,
// so we export a dummy global variable.
#ifdef __APPLE__
namespace hustle {
extern constexpr int kDarwinGlobalDummy = 0;
}
#endif
