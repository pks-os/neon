#!/bin/bash
set -x

cd /ext-src || exit 2
FAILED=
LIST=$( (echo -e "${SKIP//","/"\n"}"; ls -d -- *-src) | sort | uniq -u)
for d in ${LIST}
do
       [ -d "${d}" ] || continue
       if ! psql -w -c "select 1" >/dev/null; then
          FAILED="${d} ${FAILED}"
          break
       fi
       USE_PGXS=1 make -C "${d}" installcheck || FAILED="${d} ${FAILED}"
done
[ -z "${FAILED}" ] && exit 0
echo "${FAILED}"
exit 1