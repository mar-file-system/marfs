# Isa-l Multithreaded Benchmark

ec\_parallel\_benchmark options: <br/>
<br/>
-D 		Enable RDMA server<br/>
-s 		RDMA server name (Required if -D specified)<br/>
-P &lt;val&gt;	RDMA server port number (Required if -D specified)<br/>
-N &lt;val&gt;	Number of NUMA nodes (Must be specified if NUMA awareness is desired)<br/>
-n &lt;val&gt;	Number of CPUs per NUMA node (Must be specified if -N present)<br/>
-c &lt;val&gt;	Compression option: 0 - No compression; 1 - Compression before encode; 2 - Compression after encode<br/>
-C &lt;val&gt;	CRC option: 0 - no CRC; 1 - CRC first; 2 - CRC between compression/encode; 3 - CRC after both compression/encode<br/>
-R &lt;val&gt;	Compression Ratio: 0 - random data; 0.25 - 4:1 compression ratio; 0.5 - 2:1 compression ratio; 0.75 - 1.3:1 compression ratio<br/>
-T &lt;val&gt;	CRC library: ZLIB - uses zlib adler32; IEEE - Intel Isa-l crc32\_ieee; RFC: Intel Isa-l crc32\_gzip\_refl<br/>
-k &lt;val&gt;	Number of data blocks<br/>
-p &lt;val&gt;	Number of parity blocks<br/>
-b &lt;val&gt;	Block size (Maximum 1M), eg 32K, 64K, 1M<br/>
-t &lt;val&gt;	Number of threads<br/>
-d &lt;val&gt;	Per-thread input data size, eg 1G, 1000G<br/>
<br/>
ec\_rdma\_client is RDMA benchmark client. It has the following options:<br/>
<br/>
-k &lt;val&gt;	Number of data blocks (MUST MATCH SERVER SIDE)<br/>
-n &lt;val&gt;	Number of client (MUST match server -t value)<br/>
-i &lt;val&gt;	Per-thread input data size, eg 1G, 500G<br/>
-b &lt;val&gt;	Block size (MUST match server -b value)<br/>
-R &lt;val&gt;	Compression Ratio, can be any value between 0 - 1<br/>
-s &lt;val&gt;	RDMA server hostname (MUST match RDMA server -s value)<br/>
-p &lt;val&gt;	Server port number (MUST match server side -P value)<br/>
-T &lt;val&gt;	CRC library: ZLIB - uses zlib adler32; IEEE - Intel Isa-l crc32\_ieee; RFC: Intel Isa-l crc32\_gzip\_refl (MUST match server side -T value)<br/>
-C &lt;val&gt;	CRC option: 0 - no CRC; 1 - CRC first; 2 - CRC between compression/encode; 3 - CRC after both compression/encode (MUST match server side -C value)<br/>
-c &lt;val&gt;  Compression option: 0 - No compression; 1 - Compression before encode; 2 - Compression after encode (MUST match server side -c value)<br/>
<br/>
NOTE: To benchmark RDMA, user must first start the server, then start client<br/>
