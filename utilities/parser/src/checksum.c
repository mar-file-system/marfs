#include <stdio.h>
#include <string.h>

#include <openssl/md5.h>

int main(int argc, char *argv[])
{
unsigned char c[MD5_DIGEST_LENGTH], data[1024];
char fn[64];
int i, bytes;
FILE *inFile;
MD5_CTX mdContext;


strcpy(fn, "./config/config-2");
inFile = fopen (fn, "rb");

if (inFile == NULL) {
   printf ("%s can't be opened.\n", fn);
   return 0;
   }

MD5_Init (&mdContext);
while ((bytes = fread (data, 1, 1024, inFile)) != 0)
   MD5_Update (&mdContext, data, bytes);

MD5_Final (c,&mdContext);
for (i = 0; i < MD5_DIGEST_LENGTH; i++)
   printf("%02x", c[i]);

printf (" %s\n", fn);

fclose (inFile);

return 0;
}
