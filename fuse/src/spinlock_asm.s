# A program to be called from a C program.
# This uses the "cmpxchg" instruction to atmoically test-and-set a value,
# such that another thread can not read the value between the time we read it
# and try to write a new value to it.  This is the basis for fast locking,
# on x86_64.
#
# see http://stackoverflow.com/questions/13901261/calling-assembly-function-from-c
# see http://www.cs.virginia.edu/~evans/cs216/guides/x86.html
#
# http://0xax.blogspot.com/2014/12/say-hello-to-x8664-assembly-part-7.html
# says that the first six parms are passed in rdi, rsi, rdx, rcx, r8, and r9,
# respectively.  Everyone else tells you how to use the stack.  Turns out,
# everyone else is wrong.  Caller from C does not use the stack.  Instead,
# our parameter is in rdi!


# Declaring data that doesn't change
.section .data
string: .ascii  "Hello from assembler\n"
length: .quad   . - string

   
# The code
.section .text
.global spin_lock
.type   spin_lock, @function              #<-Important

spin_lock:
   pushq    %rbp                   # save caller's stack base-pointer
   movq     %rsp,%rbp              # our base-pointer is the current stack-pointer

   mov      $1,%edx

spin:
   # spin until the lock-location holds zero
   # (then store that in EAX)

   mov     (%rdi),%eax             # parm1 is ptr to lock-value
   test    %eax,%eax
   jnz     spin                   # someone else has the lock

   # # Now, EAX is zero, loaded from (RDI)
   # # try to set lock to 1, assuring nobody else (using this code) does the same
   # if EAX == (destination):
   #    destination <-- source (i.e. EDX)
   #    EFLAGS      <-- (EAX - destination)  i.e. is-zero
   # else
   #    EAX         <-- destination
   #    EFLAGS      <-- (EAX - destination)
   
 lock cmpxchg %edx,(%rdi)     # atomic try-lock src,dest
   test     %eax,%eax
   jnz      spin               # someone got in first

return:
   movq %rsp,%rbp
   popq %rbp

   ret
