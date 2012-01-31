#include "dec_util.h"

#include <mysql/m_string.h>
#include <mysql/my_sys.h>

namespace slave
{

namespace dec_util
{


#define DIG_PER_DEC1 9
#define DIG_BASE     1000000000
#define DIG_MAX      (DIG_BASE-1)

#define mi_sint1korr(A) ((int8)(*A))
#define mi_uint1korr(A) ((uint8)(*A))

#define mi_sint2korr(A) ((int16) (((int16) (((uchar*) (A))[1])) +\
                                  ((int16) ((int16) ((char*) (A))[0]) << 8)))
#define mi_sint3korr(A) ((int32) (((((uchar*) (A))[0]) & 128) ? \
                                  (((uint32) 255L << 24) | \
                                   (((uint32) ((uchar*) (A))[0]) << 16) |\
                                   (((uint32) ((uchar*) (A))[1]) << 8) | \
                                   ((uint32) ((uchar*) (A))[2])) : \
                                  (((uint32) ((uchar*) (A))[0]) << 16) |\
                                  (((uint32) ((uchar*) (A))[1]) << 8) | \
                                  ((uint32) ((uchar*) (A))[2])))
#define mi_sint4korr(A) ((int32) (((int32) (((uchar*) (A))[3])) +\
                                  ((int32) (((uchar*) (A))[2]) << 8) +\
                                  ((int32) (((uchar*) (A))[1]) << 16) +\
                                  ((int32) ((int16) ((char*) (A))[0]) << 24)))

#define FIX_INTG_FRAC_ERROR(len, intg1, frac1, error)                   \
        do                                                              \
        {                                                               \
          if (unlikely(intg1+frac1 > (len)))                            \
          {                                                             \
            if (unlikely(intg1 > (len)))                                \
            {                                                           \
              intg1=(len);                                              \
              frac1=0;                                                  \
              error=E_DEC_OVERFLOW;                                     \
            }                                                           \
            else                                                        \
            {                                                           \
              frac1=(len)-intg1;                                        \
              error=E_DEC_TRUNCATED;                                    \
            }                                                           \
          }                                                             \
          else                                                          \
            error=E_DEC_OK;                                             \
        } while(0)


static const int dig2bytes[DIG_PER_DEC1+1]={0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
static const dec1 powers10[DIG_PER_DEC1+1]={1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

static double scaler10[]= {1.0, 1e10, 1e20, 1e30, 1e40, 1e50, 1e60, 1e70, 1e80, 1e90};
static double scaler1[]= {1.0, 10.0, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9};

int decimal_bin_size(int precision, int scale)
{
    int intg=precision-scale,
        intg0=intg/DIG_PER_DEC1, frac0=scale/DIG_PER_DEC1,
        intg0x=intg-intg0*DIG_PER_DEC1, frac0x=scale-frac0*DIG_PER_DEC1;

    DBUG_ASSERT(scale >= 0 && precision > 0 && scale <= precision);
    return intg0*sizeof(dec1)+dig2bytes[intg0x]+
        frac0*sizeof(dec1)+dig2bytes[frac0x];
}

int bin2dec(const char *from, decimal_t *to, int precision, int scale)
{
    int error=E_DEC_OK, intg=precision-scale,
            intg0=intg/DIG_PER_DEC1, frac0=scale/DIG_PER_DEC1,
            intg0x=intg-intg0*DIG_PER_DEC1, frac0x=scale-frac0*DIG_PER_DEC1,
            intg1=intg0+(intg0x>0), frac1=frac0+(frac0x>0);
    dec1 *buf=to->buf, mask=(*from & 0x80) ? 0 : -1;
    const char *stop;
    char *d_copy;
    int bin_size= decimal_bin_size(precision, scale);

    //sanity(to);
    d_copy= (char*) my_alloca(bin_size);
    memcpy(d_copy, from, bin_size);
    d_copy[0]^= 0x80;
    from= d_copy;

    FIX_INTG_FRAC_ERROR(to->len, intg1, frac1, error);
    if (unlikely(error))
    {
        if (intg1 < intg0+(intg0x>0))
        {
            from+=dig2bytes[intg0x]+sizeof(dec1)*(intg0-intg1);
            frac0=frac0x=intg0x=0;
            intg0=intg1;
        }
        else
        {
            frac0x=0;
            frac0=frac1;
        }
    }

    to->sign=(mask != 0);
    to->intg=intg0*DIG_PER_DEC1+intg0x;
    to->frac=frac0*DIG_PER_DEC1+frac0x;

    if (intg0x)
    {
        int i=dig2bytes[intg0x];
        dec1 UNINIT_VAR(x);
        switch (i)
        {
            case 1: x=mi_sint1korr(from); break;
            case 2: x=mi_sint2korr(from); break;
            case 3: x=mi_sint3korr(from); break;
            case 4: x=mi_sint4korr(from); break;
            default: DBUG_ASSERT(0);
        }
        from+=i;
        *buf=x ^ mask;
        if (((ulonglong)*buf) >= (ulonglong) powers10[intg0x+1])
            goto err;
        if (buf > to->buf || *buf != 0)
            buf++;
        else
            to->intg-=intg0x;
    }
    for (stop=from+intg0*sizeof(dec1); from < stop; from+=sizeof(dec1))
    {
        DBUG_ASSERT(sizeof(dec1) == 4);
        *buf=mi_sint4korr(from) ^ mask;
        if (((uint32)*buf) > DIG_MAX)
            goto err;
        if (buf > to->buf || *buf != 0)
            buf++;
        else
            to->intg-=DIG_PER_DEC1;
    }
    DBUG_ASSERT(to->intg >=0);
    for (stop=from+frac0*sizeof(dec1); from < stop; from+=sizeof(dec1))
    {
        DBUG_ASSERT(sizeof(dec1) == 4);
        *buf=mi_sint4korr(from) ^ mask;
        if (((uint32)*buf) > DIG_MAX)
            goto err;
        buf++;
    }
    if (frac0x)
    {
        int i=dig2bytes[frac0x];
        dec1 UNINIT_VAR(x);
        switch (i)
        {
            case 1: x=mi_sint1korr(from); break;
            case 2: x=mi_sint2korr(from); break;
            case 3: x=mi_sint3korr(from); break;
            case 4: x=mi_sint4korr(from); break;
            default: DBUG_ASSERT(0);
        }
        *buf=(x ^ mask) * powers10[DIG_PER_DEC1 - frac0x];
        if (((uint32)*buf) > DIG_MAX)
            goto err;
        buf++;
    }
    my_afree(d_copy);

    if (to->intg == 0 && to->frac == 0)
        decimal_make_zero(to);
    return error;

err:
    my_afree(d_copy);
    decimal_make_zero(to);
    return(E_DEC_BAD_NUM);
}

void dec2dbl(decimal_t *from, double *to)
{
    double result= 0.0;
    int i, exp= 0;
    dec1 *buf= from->buf;

    for (i= from->intg; i > 0; i-= DIG_PER_DEC1)
        result= result * DIG_BASE + *buf++;

    for (i= from->frac; i > 0; i-= DIG_PER_DEC1) {
        result= result * DIG_BASE + *buf++;
        exp+= DIG_PER_DEC1;
    }

    result/= scaler10[exp / 10] * scaler1[exp % 10];

    *to= from->sign ? -result : result;
}


} // namespace dec_util

} // namespace slave
