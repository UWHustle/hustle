//
// Created by SURYADEV on 06/03/21.
//
#include <string>
#include <algorithm>

#ifndef HUSTLE_STRING_UTILS_H
#define HUSTLE_STRING_UTILS_H


class StringUtils {
public:
    inline static std::string trim(const std::string& s) {
        std::string trimmed_str;
        std::stringstream stream(s);
        stream >> trimmed_str;
        return trimmed_str;
    }

};


#endif //HUSTLE_STRING_UTILS_H
