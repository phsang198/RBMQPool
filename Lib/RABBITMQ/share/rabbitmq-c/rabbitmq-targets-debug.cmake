#----------------------------------------------------------------
# Generated CMake target import file for configuration "Debug".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "rabbitmq::rabbitmq" for configuration "Debug"
set_property(TARGET rabbitmq::rabbitmq APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(rabbitmq::rabbitmq PROPERTIES
  IMPORTED_IMPLIB_DEBUG "${_IMPORT_PREFIX}/debug/lib/rabbitmq.4.lib"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/debug/bin/rabbitmq.4.dll"
  )

list(APPEND _cmake_import_check_targets rabbitmq::rabbitmq )
list(APPEND _cmake_import_check_files_for_rabbitmq::rabbitmq "${_IMPORT_PREFIX}/debug/lib/rabbitmq.4.lib" "${_IMPORT_PREFIX}/debug/bin/rabbitmq.4.dll" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
