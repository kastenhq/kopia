#!/usr/bin/env bash
#
# Runs kopia operations with memory tracing and profiling enabled
# Requires GNU time at /usr/bin/time

set -o errexit
set -o nounset
set -o xtrace

export GOOGLE_APPLICATION_CREDENTIALS=/mnt/creds/kasten-gke-sa.json

readonly KOPIA_TOKEN=$(cat /mnt/creds/kopia-token)

TS=$(date +%Y%m%d-%H%M%S)

readonly OUTPUT_BASE=/data/exp-out
readonly OUTPUT_DIR="${OUTPUT_BASE}/${TS}"
mkdir -p "${OUTPUT_DIR}"

readonly OUT_FILE_BASE=$(basename ${0})

# Capture stdout and stderr
exec 3>&1 4>&2
exec 1>"${OUTPUT_DIR}/${OUT_FILE_BASE}-${TS}-out.txt" 2>"${OUTPUT_DIR}/${OUT_FILE_BASE}-${TS}-err.txt"

readonly APPEND_FIO="append-10g.fio"
readonly WRITE_FIO="write-10g.fio"
readonly CONFIG_DIR=/mnt/config
readonly DATA_DIR=/data/fio
readonly KOPIA_LOG_DIR="${OUTPUT_BASE}/kopia-log"
readonly KOPIA_PARALLEL=10
readonly -a KOPIA_FLAGS=(
#    --file-log-level=debug
    --log-level=error
    --log-dir="${KOPIA_LOG_DIR}"
    --parallel=${KOPIA_PARALLEL}
    --profile-memory=524288
#    --track-memory-usage=30s
)

readonly -a TIME_FLAGS=(
    --append
    --output="${OUTPUT_DIR}/kopia-time.txt"
    --verbose
)

PATH=${PATH}:/kopia

# Capture the script and config parameters
cp "${0}" "${OUTPUT_DIR}"
cp "${CONFIG_DIR}"/* "${OUTPUT_DIR}"

ulimit -a

mkdir -p "${DATA_DIR}" "${KOPIA_LOG_DIR}"

# connect kopia to repo
/usr/bin/time "${TIME_FLAGS[@]}" \
    kopia repository connect from-config --token "${KOPIA_TOKEN}"

du -sh "${DATA_DIR}"
mkdir -p "${DATA_DIR}/${TS}"
cd "${DATA_DIR}/${TS}"
# Generate initial data
fio --output="${DATA_DIR}/output-${TS}-0-seed.json" --output-format=json+ --directory="${DATA_DIR}/${TS}/write-phase" "${CONFIG_DIR}/${WRITE_FIO}"
du -sh "${DATA_DIR}"

# create initial snapshot and measure resource consumption
/usr/bin/time "${TIME_FLAGS[@]}" \
    kopia snapshot create "${DATA_DIR}" \
    "${KOPIA_FLAGS[@]}" \
    --profile-dir="${OUTPUT_DIR}/prof/0" \
    --description="${TS} 0: Initial snapshot after seed"

#if false ; then

for i in $(seq 10)
do
    echo "Iteration ${i}"
    mkdir -p "${DATA_DIR}/${TS}/${i}"
    cd "${DATA_DIR}/${TS}/${i}"
    # generate addtional data
    fio --output="${DATA_DIR}/output-${TS}-${i}.json" --output-format=json+ "${CONFIG_DIR}/${APPEND_FIO}"
    du -sh "${DATA_DIR}"
    # create snapshot and measure resource consumption
    /usr/bin/time "${TIME_FLAGS[@]}" \
        kopia snapshot create "${DATA_DIR}" \
        "${KOPIA_FLAGS[@]}" \
        --profile-dir="${OUTPUT_DIR}/prof/${i}" \
        --description="${TS} ${i}: Incremental snapshot"
    # measure memory needed to load indices and perform a tree walk
    /usr/bin/time "${TIME_FLAGS[@]}" \
        kopia snapshot create "${DATA_DIR}" \
        "${KOPIA_FLAGS[@]}" \
        --profile-dir="${OUTPUT_DIR}/prof/${i}-d0" \
        --description="${TS} ${i}: Incremental snapshot 0 delta"
done

#fi

# Close output files
exec 1>&- 2>&-
# Restore stdout and stderr
exec 1>&3 2>&4

# Upload logs, profile and output files: Kopia FTW
/usr/bin/time --verbose kopia snapshot create "${OUTPUT_BASE}" --description="${TS}: output"

echo 'Done!'
