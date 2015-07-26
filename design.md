Data representation
===
The data is largely made of nucleobase strings. Known nucleobases are A, G, C, T 

When storing the data in Redis, we'll use 2 bits to encode a single nucleobase to optimize memory consumption. 

Nucleobase enconding (NE)
---
```
00 -> A
01 -> G
10 -> C
11 -> T
```

Input Keys
---
Input key (K) is a 31 bytes string of nucleobases. Example: 
`AAAAAAAAAAAAAAAAAAAAAAAACCCCAAA`

After encoding K:
`sizeof(NE(K)) = 31 * 2 bits = 62 bits`

We'll use two additional bits set to 1 as LSB as padding for byte (8 bytes) alignment. This is done by appending 'T' to the `k`.

Values
---
Input value (V) is made of:
  1. Freq (F) is an integer = 4 bytes
  2. IE (I) is a 0..4 string of nucleobases
  3. OE (O) is a 0..4 string of nucleobases

To store the value, the following 56 bit record (R) will be used:
* 32 bits for F - 0..31
* 4 bits for I size - 32..35
* 4 bits for O size - 36..39
* 8 bits for I value - 40..47
* 8 bits for O value - 48..53

Note: although only 3 bits are needed for IE and OE sizes, we'll use a 4 to align the record bytewise

Redis sizing
===

Hash buckets
---
To reduce the consumption of memory due to the number of keys in the database the input KV pairs will be stored in hash "buckets". The key of each such hash (hK) is obtained using the function `h(NE(K))`, where `h` is the hash function crc32. Since crc32's output checksum is 32 bits long and we're interested in keeping the keyspace to 2^30 keys, we'll use only the 30 most significant bits of the checksum as `hK`.

P8 Redis overheads
---
* Per hash bucket key: 36 bytes
* Ziplist: 12 + 18 * n bytes

Data access
===
Getting the value for K is done by:
1. `R = HGET <hK> <NE(K)>`
2. Client-side bit manipulation of the record to get properties:

And optionally, setting the value with:
3. Modify the properties
4. Encode new value record:
5. `HSET <hk> <NE(K)> <R(V)>`