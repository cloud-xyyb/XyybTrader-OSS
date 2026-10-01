#include "xt_util/string/XtStringUtil.h"

#include <cctype>
#include <cstring>

#if defined(__linux__)
#include <iconv.h>
#endif

namespace XtStringUtil {

std::string lower(const std::string& s) {
    std::string r = s;
    for (char& c : r) c = static_cast<char>(::tolower(static_cast<unsigned char>(c)));
    return r;
}

std::string upper_without_space(const std::string& s) {
    std::string r;
    r.reserve(s.size());
    for (char c : s) {
        if (std::isspace(static_cast<unsigned char>(c))) continue;
        r.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
    }
    return r;
}

std::string trim(const std::string& s) {
    size_t b = s.find_first_not_of(" \t\r\n");
    if (b == std::string::npos) return "";
    size_t e = s.find_last_not_of(" \t\r\n");
    return s.substr(b, e - b + 1);
}

std::string strip_dot_prefix(const std::string& s) {
    const size_t p = s.find('.');
    if (p == std::string::npos) return s;
    return s.substr(p + 1);
}

bool parse_double(const std::string& s, double& out) {
    std::string v = trim(s);
    if (v.empty())
        return false;
    try {
        size_t pos = 0;
        const double parsed = std::stod(v, &pos);
        if (pos != v.size()) return false;
        out = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

std::string gbk_to_utf8_or_original(const char* s) {
    if (!s || !s[0]) return "";
#if defined(__linux__)
    iconv_t cd = iconv_open("UTF-8", "GBK");
    if (cd == (iconv_t)-1) {
        return s;
    }

    size_t in_bytes = std::strlen(s);
    size_t out_bytes = in_bytes * 3 + 1;
    std::string out;
    out.resize(out_bytes);

    char* inbuf = const_cast<char*>(s);
    char* outbuf = out.data();
    size_t out_left = out_bytes;
    const size_t ret = iconv(cd, &inbuf, &in_bytes, &outbuf, &out_left);
    iconv_close(cd);
    if (ret == static_cast<size_t>(-1)) {
        return s;
    }

    out.resize(out_bytes - out_left);
    return out;
#else
    return s;
#endif
}

}  // namespace XtStringUtil
