#ifndef __SLAVE_DEC_UTIL_H_
#define __SLAVE_DEC_UTIL_H_

/*
    for storage decimal numbers are converted to the "binary" format.

    This format has the following properties:
      1. length of the binary representation depends on the {precision, scale}
      as provided by the caller and NOT on the intg/frac of the decimal to
      convert.
      2. binary representations of the same {precision, scale} can be compared
      with memcmp - with the same result as decimal_cmp() of the original
      decimals (not taking into account possible precision loss during
      conversion).

    This binary format is as follows:
      1. First the number is converted to have a requested precision and scale.
      2. Every full DIG_PER_DEC1 digits of intg part are stored in 4 bytes
         as is
      3. The first intg % DIG_PER_DEC1 digits are stored in the reduced
         number of bytes (enough bytes to store this number of digits -
         see dig2bytes)
      4. same for frac - full decimal_digit_t's are stored as is,
         the last frac % DIG_PER_DEC1 digits - in the reduced number of bytes.
      5. If the number is negative - every byte is inversed.
      5. The very first bit of the resulting byte array is inverted (because
         memcmp compares unsigned bytes, see property 2 above)

    Example:

      1234567890.1234

    internally is represented as 3 decimal_digit_t's

      1 234567890 123400000

    (assuming we want a binary representation with precision=14, scale=4)
    in hex it's

      00-00-00-01  0D-FB-38-D2  07-5A-EF-40

    now, middle decimal_digit_t is full - it stores 9 decimal digits. It goes
    into binary representation as is:


      ...........  0D-FB-38-D2 ............

    First decimal_digit_t has only one decimal digit. We can store one digit in
    one byte, no need to waste four:

                01 0D-FB-38-D2 ............

    now, last digit. It's 123400000. We can store 1234 in two bytes:

                01 0D-FB-38-D2 04-D2

    So, we've packed 12 bytes number in 7 bytes.
    And now we invert the highest bit to get the final result:

                81 0D FB 38 D2 04 D2

    And for -1234567890.1234 it would be

                7E F2 04 37 2D FB 2D
*/

#include <mysql/my_global.h>
#include <mysql/decimal.h>

namespace slave
{

namespace dec_util
{

typedef decimal_digit_t dec1;

// converts from raw const char* to internal MySql type decimal_t
int bin2dec(const char *from, decimal_t *to, int precision, int scale);

// converts from internal MySql type decimal_t to double
void dec2dbl(decimal_t *from, double *to);

} // namespace dec_util

} // namespace slave

#endif
