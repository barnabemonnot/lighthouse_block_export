from collections import defaultdict
import csv
import sys
import math
import argparse
import datetime

import plyvel
import eth2spec.phase0.spec as spec

# See https://github.com/sigp/lighthouse/blob/ce10db15da0db4cbe76b96b58ccd1b40e39ed124/beacon_node/store/src/lib.rs#L195-L215
BLOCK_PREFIX = b"blk"

BLOCK_COLS = ['state_root', 'slot', 'proposer_index']


def extract_block(sbb: spec.SignedBeaconBlock):
    return (sbb.message.state_root, sbb.message.slot, sbb.message.proposer_index)


ATTESTATION_COLS = [
    'slot',
    'beacon_block_root',
    'attesting_indices',
    'source_epoch',
    'source_block_root',
    'target_epoch',
    'target_block_root',
]


def bitlist_to_str(bitlist: spec.Bitlist):
    return ''.join('1' if b else '0' for b in bitlist)


def extract_attestations(sbb: spec.SignedBeaconBlock):
    return [(
        sbb.message.slot,
        a.data.beacon_block_root,
        bitlist_to_str(a.aggregation_bits),
        a.data.source.epoch,
        a.data.source.root,
        a.data.target.epoch,
        a.data.target.root
    ) for a in sbb.message.body.attestations.readonly_iter()]


DEPOSIT_COLS = [
    'slot',
    'pubkey',
    'amount'
]


def extract_deposits(sbb: spec.SignedBeaconBlock):
    return [(
        sbb.message.slot,
        d.data.pubkey,
        d.data.amount
    ) for d in sbb.message.body.deposits.readonly_iter()]


EXIT_COLS = [
    'slot',
    'exit_epoch',
    'validator_index'
]


def extract_exits(sbb: spec.SignedBeaconBlock):
    return [(
        sbb.message.slot,
        e.message.epoch,
        e.message.validator_index
    ) for e in sbb.message.body.voluntary_exits.readonly_iter()]


def print_time():
    print(datetime.datetime.now())

def write_data(out_dir, count, step_size, blocks, attestations, deposits, exits):
    block_file = f"{out_dir}/blocks_{count // step_size}.csv"
    with open(block_file, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(BLOCK_COLS)
        for block in blocks:
            writer.writerow(block)

    attestation_file = f"{out_dir}/attestations_{count // step_size}.csv"
    with open(attestation_file, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(ATTESTATION_COLS)
        for attestation in attestations:
            writer.writerow(attestation)

    deposit_file = f"{out_dir}/deposits_{count // step_size}.csv"
    with open(deposit_file, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(DEPOSIT_COLS)
        for deposit in deposits:
            writer.writerow(deposit)

    exit_file = f"{out_dir}/exits_{count // step_size}.csv"
    with open(exit_file, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(EXIT_COLS)
        for exit in exits:
            writer.writerow(exit)

def export_blocks(lighthouse_dir, out_dir, start_slot=0, end_slot=math.inf, step_size=1000):
    # start_slot is inclusive, end_slot is exclusive
    db_dir = f"{lighthouse_dir}/beacon/chain_db"
    db = plyvel.DB(db_dir)
    blocks_db = db.prefixed_db(BLOCK_PREFIX)

    blocks = []
    attestations = []
    deposits = []
    exits = []
    highest_slot = 1

    try:
        count = 0
        db_count = 0
        for key, value in blocks_db:
            db_count += 1
            signed_beacon_block = spec.SignedBeaconBlock.decode_bytes(value)

            block_slot = signed_beacon_block.message.slot
            if block_slot > highest_slot:
                highest_slot = block_slot

            if db_count % step_size == 0:
                print(f"{datetime.datetime.now()}: seen {db_count} blocks out of approx. {highest_slot}")

            if block_slot < start_slot or block_slot >= end_slot:
                continue

            count += 1

            blocks.append(extract_block(signed_beacon_block))
            attestations.extend(extract_attestations(signed_beacon_block))
            deposits.extend(extract_deposits(signed_beacon_block))
            exits.extend(extract_exits(signed_beacon_block))

            if count % step_size == 0:
                print_time()
                print(f'{count} blocks processed')

                write_data(out_dir, count, step_size, blocks, attestations, deposits, exits)

                blocks = []
                attestations = []
                deposits = []
                exits = []

        # One last write for the last batch
        write_data(out_dir, count, step_size, blocks, attestations, deposits, exits)
        return blocks

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-d", "--datadir", help="Lighthouse data directory")
    parser.add_argument("-o", "--outdir", help="Output directory")
    parser.add_argument("-s", "--stepsize", help="Step size")
    parser.add_argument("-st", "--startslot", help="Start slot")
    parser.add_argument("-en", "--endslot", help="End slot")

    args = parser.parse_args()
    print(args)

    if not args.datadir or not args.outdir:
        print('Usage: `python export.py -d $LIGHTHOUSE_DIR -o $OUTPUT_DIR`')

    else:
        lighthouse_dir = args.datadir
        out_dir = args.outdir

        if args.startslot:
            start_slot = int(args.startslot)
        else:
            start_slot = 0

        if args.endslot:
            end_slot = int(args.endslot)
        else:
            end_slot = math.inf

        if args.stepsize:
            step_size = int(args.stepsize)
        else:
            step_size = 1000

        export_blocks(lighthouse_dir, out_dir, start_slot=start_slot, end_slot=end_slot, step_size=step_size)
