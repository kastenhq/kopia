#!/usr/bin/env sh

set -o errexit
set -o nounset
set -o xtrace

TS=$(date +%Y%m%d-%H%M%S)

OUTPUT_BASE=/data/exp-out
OUTPUT_DIR="${OUTPUT_BASE}/${TS}"
mkdir -p "${OUTPUT_DIR}"

OUT_FILE_BASE=$(basename ${0})

# Capture stdout and stderr
exec 3>&1 4>&2
exec 1>"${OUTPUT_DIR}/${OUT_FILE_BASE}-${TS}-out.txt" 2>"${OUTPUT_DIR}/${OUT_FILE_BASE}-${TS}-err.txt"

export GOOGLE_APPLICATION_CREDENTIALS=/mnt/creds/kasten-gke-sa.json
DATA_DIR=/data/fio
KOPIA_PARALLEL=10
CONFIG_DIR=/mnt/config
PATH=${PATH}:/kopia

# Capture the script and config parameters
cp "${0}" "${OUTPUT_DIR}"
cp "${CONFIG_DIR}"/* "${OUTPUT_DIR}"

mkdir -p "${DATA_DIR}"

KOPIA_LOG_DIR="${OUTPUT_BASE}/kopia-log"
mkdir -p "${KOPIA_LOG_DIR}"

# connect kopia to repo
kopia repository connect from-config --token $(cat /mnt/creds/kopia-token)

du -sh "${DATA_DIR}"
# run data seed
fio --output="${DATA_DIR}/output-${TS}-0-seed.json" --output-format=json+ ${CONFIG_DIR}/seed-100g.fio
du -sh "${DATA_DIR}"

# create initial snapshot and measure resource consumption
/usr/bin/time -v kopia snapshot create "${DATA_DIR}" \
    --log-level=error \
    --log-dir="${KOPIA_LOG_DIR}" \
    --parallel=${KOPIA_PARALLEL} \
    --description="${TS} 0: Initial snapshot after seed"

for i in $(seq 10)
do
    echo "Iteration ${i}"
    # generate addtional data
    fio --output="${DATA_DIR}/output-${TS}-${i}.json" --output-format=json+ ${CONFIG_DIR}/append-10g.fio
    du -sh "${DATA_DIR}"
    # create snapshot and measure resource consumption
    /usr/bin/time -v kopia snapshot create "${DATA_DIR}" \
        --log-level=error \
        --log-dir="${KOPIA_LOG_DIR}" \
        --parallel=${KOPIA_PARALLEL} \
        --description="${TS} ${i}: Incremental snapshot"
    # measure memory needed to load indices
    /usr/bin/time -v kopia snapshot create "${DATA_DIR}" \
        --log-level=error \
        --log-dir="${KOPIA_LOG_DIR}" \
        --parallel=${KOPIA_PARALLEL} \
        --description="${TS} ${i}: Incremental snapshot 0 delta"
done

# Close output files
exec 1>&- 2>&-
# Restore stdout and stderr
exec 1>&3 2>&4

# Upload logs: Kopia FTW
/usr/bin/time -v kopia snapshot create "${OUTPUT_BASE}" --description="${TS}: output"

echo 'Done!'
