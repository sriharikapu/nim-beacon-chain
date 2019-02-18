import
  options,
  chronicles, eth/[rlp, p2p], chronos, ranges/bitranges, eth/p2p/rlpx,
  spec/[datatypes, crypto, digest],
  beacon_node, beacon_chain_db, time

type
  ValidatorChangeLogEntry* = object
    case kind*: ValidatorSetDeltaFlags
    of Activation:
      pubkey: ValidatorPubKey
    else:
      index: uint32

  ValidatorSet = seq[Validator]

  BeaconSyncState* = ref object
    node*: BeaconNode
    db*: BeaconChainDB

const maxBlocksInRequest = 50

p2pProtocol BeaconSync(version = 1,
                       shortName = "bcs",
                       networkState = BeaconSyncState):
  proc status(peer: Peer, protocolVersion, networkId: int, latestFinalizedRoot: Eth2Digest,
        latestFinalizedEpoch: uint64, bestRoot: Eth2Digest, bestSlot: uint64) =
    discard

  requestResponse:
    proc getValidatorChangeLog(peer: Peer, changeLogHead: Eth2Digest) =
      var bb: BeaconBlock
      var bs: BeaconState
      # TODO: get the changelog from the DB.
      await peer.validatorChangeLog(reqId, bb, bs, [], [], @[])

    proc validatorChangeLog(peer: Peer,
                            signedBlock: BeaconBlock,
                            beaconState: BeaconState,
                            added: openarray[ValidatorPubKey],
                            removed: openarray[uint32],
                            order: seq[byte])

  requestResponse:
    proc getBlocks(peer: Peer, fromHash: Eth2Digest, num: int = 1) =
      let step = if num < 0: -1 else: 1
      let num = abs(num)
      if num > maxBlocksInRequest or num == 0:
        # TODO: drop this peer
        assert(false)

      let db = peer.networkState.db
      var blk: BeaconBlock
      var response = newSeqOfCap[BeaconBlock](num)

      if db.getBlock(fromHash, blk):
        response.add(blk)
        var slot = int64(blk.slot)
        let targetSlot = slot + step * (num - 1)
        while slot != targetSlot:
          if slot < 0 or not db.getBlock(uint64(slot), blk):
            break
          response.add(blk)
          slot += step

      await peer.blocks(reqId, response)

    proc blocks(peer: Peer, blocks: openarray[BeaconBlock])

type
  # A bit shorter names for convenience
  ChangeLog = BeaconSync.validatorChangeLog
  ChangeLogEntry = ValidatorChangeLogEntry

proc applyBlocks(node: BeaconNode, blocks: openarray[BeaconBlock]) =
  debug "sync blocks received", count = blocks.len
  for b in blocks:
    node.processBlock(b)

proc fullSync*(node: BeaconNode) {.async.} =
  while true:
    let curSlot = node.beaconState.slot
    var targetSlot = node.beaconState.getSlotFromTime()
    debug "Syncing", curSlot, targetSlot
    assert(targetSlot >= curSlot)

    let numBlocksToDownload = min(maxBlocksInRequest.uint64, targetSlot - curSlot)
    if numBlocksToDownload == 0:
      info "Full sync complete"
      break

    var p = node.network.randomPeerWith(BeaconSync)
    if p.isNil:
      info "Waiting for more peers to sync"
      await sleepAsync(2000)
    else:
      let blks = await p.getBlocks(node.headBlockRoot, numBlocksToDownload.int)
      if blks.isSome:
        node.applyBlocks(blks.get.blocks)

func validate*(log: ChangeLog): bool =
  # TODO:
  # Assert that the number of raised bits in log.order (a.k.a population count)
  # matches the number of elements in log.added
  # https://en.wikichip.org/wiki/population_count
  return true

iterator changes*(log: ChangeLog): ChangeLogEntry =
  var
    bits = log.added.len + log.removed.len
    addedIdx = 0
    removedIdx = 0

  template nextItem(collection): auto =
    let idx = `collection Idx`
    inc `collection Idx`
    log.collection[idx]

  for i in 0 ..< bits:
    yield if log.order.getBit(i):
      ChangeLogEntry(kind: Activation, pubkey: nextItem(added))
    else:
      ChangeLogEntry(kind: ValidatorSetDeltaFlags.Exit, index: nextItem(removed))

proc getValidatorChangeLog*(node: EthereumNode, changeLogHead: Eth2Digest):
                            Future[(Peer, ChangeLog)] {.async.} =
  while true:
    let peer = node.randomPeerWith(BeaconSync)
    if peer == nil: return

    let res = await peer.getValidatorChangeLog(changeLogHead, timeout = 1)
    if res.isSome:
      return (peer, res.get)

proc applyValidatorChangeLog*(log: ChangeLog,
                              outBeaconState: var BeaconState): bool =
  # TODO:
  #
  # 1. Validate that the signedBlock state root hash matches the
  #    provided beaconState
  #
  # 2. Validate that the applied changelog produces the correct
  #    new change log head
  #
  # 3. Check that enough signatures from the known validator set
  #    are present
  #
  # 4. Apply all changes to the validator set
  #

  outBeaconState.finalized_epoch =
    log.signedBlock.slot div EPOCH_LENGTH

  outBeaconState.validator_registry_delta_chain_tip =
    log.beaconState.validator_registry_delta_chain_tip

