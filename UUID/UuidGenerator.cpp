#include "uuid_4.h"
#include "UuidGenerator.h"

namespace UUID
{
	bool _bInited = false;

	void Init()
	{
		uuid4_init();
		_bInited = true;
	}

	auto Generate() -> std::string
	{
		if (!_bInited)
			Init();

		char dst[UUID4_LEN];
		uuid4_generate(dst);
		return dst;
	}
}
