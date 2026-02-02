from ddm_sdk.models.blockchain import IngestTxBody
from helpers import poll_task_until_ready, task_id_from_taskref


def test_ingest_signed_tx(client, web3, private_key, network):
    """
    Uses a REAL signed tx, then waits for backend indexing.
    """

    # example: already prepared reward/suite tx data
    to = "0x6F85bb299D498010FC47253B19a63D13021f4010"
    data = "0x"  # replace with encoded calldata

    acct = web3.eth.account.from_key(private_key)

    tx = {
        "from": acct.address,
        "to": to,
        "data": data,
        "nonce": web3.eth.get_transaction_count(acct.address),
        "chainId": web3.eth.chain_id,
        "gasPrice": web3.eth.gas_price,
        "gas": 300_000,
    }

    signed = acct.sign_transaction(tx)
    tx_hash = web3.eth.send_raw_transaction(signed.rawTransaction).hex()

    task = client.blockchain.ingest_tx(
        IngestTxBody(
            network=network,
            address=to,
            tx_hash=tx_hash,
        )
    )

    tid = task_id_from_taskref(task)
    assert tid

    st = poll_task_until_ready(client, tid, timeout_s=180)
    assert st.is_success(), st.error or st.message
