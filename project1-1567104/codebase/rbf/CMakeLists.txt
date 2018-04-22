cmake_minimum_required(VERSION 3.10)
project(rbf)

set(CMAKE_CXX_STANDARD 11)

include_directories(.)

add_executable(rbf
        makefile
        pfm.cc
        pfm.h
        rbfm.cc
        rbfm.h
        rbftest1.cc
        rbftest10.cc
        rbftest11.cc
        rbftest12.cc
        rbftest2.cc
        rbftest3.cc
        rbftest4.cc
        rbftest5.cc
        rbftest6.cc
        rbftest7.cc
        rbftest8.cc
        rbftest8b.cc
        rbftest9.cc
        test_util.h)
