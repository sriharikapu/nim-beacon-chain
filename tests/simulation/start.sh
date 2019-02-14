#!/bin/bash

set -eu

# Kill children on ctrl-c
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

# Set a default value for the env vars usually supplied by nimbus Makefile
: ${SKIP_BUILDS:=""}
: ${BUILD_OUTPUTS_DIR:="./build"}

NUMBER_OF_VALIDATORS=299

cd $(dirname "$0")
SIMULATION_DIR=$PWD/data
mkdir -p "$SIMULATION_DIR"

STARTUP_FILE="$SIMULATION_DIR/startup.json"
SNAPSHOT_FILE="$SIMULATION_DIR/state_snapshot.json"

cd $(git rev-parse --show-toplevel)
ROOT_DIR=$PWD

mkdir -p $BUILD_OUTPUTS_DIR

BEACON_NODE_BIN=$BUILD_OUTPUTS_DIR/beacon_node
VALIDATOR_KEYGEN_BIN=$BUILD_OUTPUTS_DIR/validator_keygen

if [[ -z "$SKIP_BUILDS" ]]; then
  nim c -o:"$VALIDATOR_KEYGEN_BIN" -d:release beacon_chain/validator_keygen
  nim c -o:"$BEACON_NODE_BIN" beacon_chain/beacon_node
fi

if [ ! -f $STARTUP_FILE ]; then
  $VALIDATOR_KEYGEN_BIN --validators=$NUMBER_OF_VALIDATORS --outputDir="$SIMULATION_DIR" --startupDelay=2
fi

if [ ! -f $SNAPSHOT_FILE ]; then
  $BEACON_NODE_BIN createChain \
    --chainStartupData:$STARTUP_FILE \
    --out:$SNAPSHOT_FILE
fi

# ⚠ if padding change the node-0 to node-00
MASTER_NODE_ADDRESS_FILE="$SIMULATION_DIR/node-00/beacon_node.address"

# Delete any leftover address files from a previous session
if [ -f $MASTER_NODE_ADDRESS_FILE ]; then
  rm $MASTER_NODE_ADDRESS_FILE
fi

for i in {00..29}; do
  BOOTSTRAP_NODES_FLAG="--bootstrapNodesFile:$MASTER_NODE_ADDRESS_FILE"

  if [[ "$i" == "00" ]]; then
    BOOTSTRAP_NODES_FLAG=""
  else
    # Wait for the master node to write out its address file
    while [ ! -f $MASTER_NODE_ADDRESS_FILE ]; do
      sleep 0.1
    done
  fi

  DATA_DIR=$SIMULATION_DIR/node-$i

  # We're missing the "XX0" validators intentionally to see how
  # how the network copes with 10% missing validators
  $BEACON_NODE_BIN \
    --dataDir:"$DATA_DIR" \
    --validator:"$SIMULATION_DIR/validator-${i}1.json" \
    --validator:"$SIMULATION_DIR/validator-${i}2.json" \
    --validator:"$SIMULATION_DIR/validator-${i}3.json" \
    --validator:"$SIMULATION_DIR/validator-${i}4.json" \
    --validator:"$SIMULATION_DIR/validator-${i}5.json" \
    --validator:"$SIMULATION_DIR/validator-${i}6.json" \
    --validator:"$SIMULATION_DIR/validator-${i}7.json" \
    --validator:"$SIMULATION_DIR/validator-${i}8.json" \
    --validator:"$SIMULATION_DIR/validator-${i}9.json" \
    --tcpPort:500$i \
    --udpPort:500$i \
    --stateSnapshot:"$SNAPSHOT_FILE" \
    $BOOTSTRAP_NODES_FLAG &
done

wait # Stop when all nodes have gone down
