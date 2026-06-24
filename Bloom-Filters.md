# What is Bloom Filter
A Bloom filter is a space-efficient probabilistic data structure that is used to test whether an item is a member of a set.  
The bloom filter will always say yes if an item is a set member. However, the bloom filter might still say yes although an item  
is not a member of the set (false positive). The items can be added to the bloom filter but the items cannot be removed.  
The bloom filter supports the following operations:
- Adding an item to the set
- Test the membership of an item in the set
# Why Bloom Filters
Data structures such as HashSet can be used in a small data set to test if an item is a member of the data set. However, the  
operation to test if an item is a member of a large dataset can be expensive. The time complexity and space complexity can be linear in the worst case.
Probabilistic data structures offer constant time complexity and constant space complexity at the expense of providing an answer that is non-deterministic.
# How does a bloom filter work?
The bloom filter data structure is a bit array of length n as shown in Figure 1. The position of the buckets is indicated by the index  
(0–9) for a bit array of length ten. All the bits in the bloom filter are set to zero when the bloom filter is initialized 
(an empty bloom filter).The bloom filter discards the value of the items but stores only a set of bits identified by the execution of hash  
functions on the item.
![image](https://github.com/yadavraganu/system_design/assets/77580939/73bf6f74-37b7-455f-9d34-9cb02f78716f)

The following operations are executed to check if an item is a member of the bloom filter:
- The item is hashed through the same k-hash functions
- The modulo n (length of bit array) operation is executed on the output of the hash functions to identify the k array positions (buckets)
- Verify if all the bits at identified buckets are set to one
There is a probability that some bits on the array are set to one multiple times due to hash collisions.
# How to add an item to the bloom filter
In Figure 2, the items red and blue are added to the bloom filter. The buckets that should be set to one for the item red are identified by
the execution of the modulo operator on the computed hash value.  
h1(red) mod 10 = 1  
h2(red) mod 10 = 3  
h3(red) mod 10 = 5  
The buckets that should be set to one for the item blue are identified by the execution of the modulo operator on the computed hash value.  
h1(blue) mod 10 = 4  
h2(blue) mod 10 = 5  
h3(blue) mod 10 = 9  
The bucket at position five is set to one by distinct items red and blue.  
![image](https://github.com/yadavraganu/system_design/assets/77580939/35ec443a-4af0-4f04-8255-cd6e2b04ae6c)

# How to check the membership of an item in the bloom filter?
The following operations are executed to check if an item is a member of the bloom filter:
- The item is hashed through the same k-hash functions
- The modulo n (length of bit array) operation is executed on the output of the hash functions to identify the k array  
  positions (buckets)
- Verify if all the bits at identified buckets are set to one
  
If any of the identified bits are set to zero, the item is not a member of the bloom filter. If all the bits are set to one,
the item might be a member of the bloom filter. The uncertainty about the membership of an item is due to the possibility of
some bits being set to one by different items or due to hash function collisions.    
The bloom filter is queried to check the membership of item blue. The buckets that should be checked are identified by the
execution of the modulo operator on the computed hash value.  
h1(blue) mod 10 = 4  
h2(blue) mod 10 = 5  
h3(blue) mod 10 = 9  
The item blue might be a member of the bloom filter as all the bits are set to one.  
The bloom filter is queried to check the membership of item black, which is not a member of the bloom filter.  
h1(black) mod 10 = 0  
h2(black) mod 10 = 3  
h3(black) mod 10 = 6  
The item black is not a member of the bloom filter as the bit at position zero is set to zero. The verification
of the remaining bits can be skipped.  
