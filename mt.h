// mt.h
#pragma once

// 64-bit version
#define NN 312
#define MM 156
#define MATRIX_A64 0xB5026F5AA96619E9ULL
#define UM 0xFFFFFFFF80000000ULL /* Most significant 33 bits */
#define LM 0x7FFFFFFFULL /* Least significant 31 bits */


/* Period parameters */  
#define N 624
#define M 397
#define MATRIX_A 0x9908b0dfUL   /* constant vector a */
#define UPPER_MASK 0x80000000UL /* most significant w-r bits */
#define LOWER_MASK 0x7fffffffUL /* least significant r bits */

class MersenneTwister {
	unsigned long *mt;	/* the array for the state vector  */
	int mti;			/* mti==N+1 means mt[N] is not initialized */

	unsigned __int64 *mt64;	/* the array for the state vector  */
	int mti64;				/* mti==N+1 means mt[N] is not initialized */

public:
	MersenneTwister ();
	~MersenneTwister ();

	void init_genrand(unsigned long s);
	void init_by_array(unsigned long init_key[], unsigned long key_length);
	unsigned long genrand_int32(void);
	long genrand_int31(void);
	double genrand_real1(void);
	double genrand_real2(void);
	double genrand_real3(void);
	double genrand_res53(void);

	void init_genrand64(unsigned __int64 seed);
	void init_by_array64(unsigned __int64 init_key[], unsigned __int64 key_length);
	unsigned __int64 genrand64_int64(void);
	__int64 genrand64_int63(void);
	double genrand64_real1(void);
	double genrand64_real2(void);
	double genrand64_real3(void);
};