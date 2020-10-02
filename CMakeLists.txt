cmake_minimum_required(VERSION 3.13)

project(Redis-ImageScout VERSION 0.1.0 DESCRIPTION "Image fingerprint indexing")

include(ExternalProject)

set(MODULE_SRCS module.cpp mvptree.cpp mvpnode.cpp)

set(CMAKE_BUILD_TYPE RelWithDebInfo)

add_library(imgscout MODULE ${MODULE_SRCS})
set_target_properties(imgscout PROPERTIES PREFIX "")
target_link_options(imgscout PRIVATE "LINKER:-shared,-Bsymbolic")

find_package(Boost 1.67 COMPONENTS program_options filesystem)

if (Boost_FOUND)

  find_library(PNGLIB png)
  if (PNGLIB-NOTFOUND)
	message(FATAL_ERROR "libpng not found")
  endif()

  find_library(JPEGLIB jpeg)
  if(JPEGLIB-NOTFOUND)
	message(FATAL_ERROR "libjpeg not found")
  endif()

  find_library(TIFFLIB tiff)
  if (TIFFLIB-NOTFOUND)
	message(FATAL_ERROR "libtiff not found")
  endif()


  ExternalProject_Add(phash
	PREFIX ${CMAKE_CURRENT_BINARY_DIR}/phash
	GIT_REPOSITORY https://github.com/starkdg/phash.git
	CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>
		-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
		-DUSE_IMAGE_HASH=ON
		-DUSE_AUDIO_HASH=OFF
		-DUSE_VIDEO_HASH=OFF
		-DUSE_TEXT_HASH=OFF)


  ExternalProject_Add(hiredis
	PREFIX ${CMAKE_CURRENT_BINARY_DIR}/hiredis
	URL https://github.com/redis/hiredis/archive/v1.0.0.tar.gz
	CONFIGURE_COMMAND ""
	BUILD_COMMAND echo make static
	BUILD_IN_SOURCE 1
	INSTALL_COMMAND make install PREFIX=<INSTALL_DIR>)

  set(HIREDIS_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/hiredis/include/hiredis)
  set(HIREDIS_LIB ${CMAKE_CURRENT_BINARY_DIR}/hiredis/lib/libhiredis.a)

  message("HIREDIS_INCLUDE_DIR = ${HIREDIS_INCLUDE_DIR}")
  message("HIREDIS_LIB = ${HIREDIS_LIB}")

  set(PHASHLIB ${CMAKE_CURRENT_BINARY_DIR}/phash/lib/libpHash.so)
  message("PHASHLIB = ${PHASHLIB}")

  add_executable(imgscoutclient imgscoutclient.cpp)
  add_dependencies(imgscoutclient hiredis phash)
  target_include_directories(imgscoutclient PRIVATE ${Boost_INCLUDE_DIRS} ${HIREDIS_INCLUDE_DIR})
  target_link_libraries(imgscoutclient ${Boost_LIBRARIES} ${PHASHLIB} ${PNGLIB} ${JPEGLIB} ${TIFFLIB} ${HIREDIS_LIB})

endif()


install(TARGETS imgscout
  DESTINATION lib/redis/modules
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)

