const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const {
    normalizeMobileMessageRequest,
    createSmsClient,
    handleQueueMessage
} = require('./dist/consumer_send_sms');

test('normalizeMobileMessageRequest exige phone', () => {
    assert.throws(
        () => normalizeMobileMessageRequest({ message: 'Teste' }),
        /phone/i
    );
});

test('normalizeMobileMessageRequest exige message', () => {
    assert.throws(
        () => normalizeMobileMessageRequest({ phone: '+5511999999999' }),
        /message/i
    );
});

test('normalizeMobileMessageRequest normaliza phone e message', () => {
    const payload = normalizeMobileMessageRequest({
        phone: ' (11) 99999-9999 ',
        message: '  Sua fatura venceu  '
    });

    assert.deepEqual(payload, {
        phone: '+5511999999999',
        message: 'Sua fatura venceu'
    });
});

test('handleQueueMessage envia ack para payload valido', async () => {
    const originalFrom = process.env.SMS_FROM_PHONE;
    process.env.SMS_FROM_PHONE = '+5511000000000';

    let sentPayload = null;
    const smsClient = {
        messages: {
            create: async (payload) => {
                sentPayload = payload;
                return { sid: 'SM123' };
            }
        }
    };

    const channel = {
        ackCalled: false,
        nackCalled: false,
        ack(msg) {
            this.ackCalled = msg === message;
        },
        nack() {
            this.nackCalled = true;
        }
    };

    const message = {
        content: Buffer.from(
            JSON.stringify({
                phone: '(11) 98888-7777',
                message: 'Oi'
            })
        )
    };

    await handleQueueMessage(channel, smsClient, message);

    assert.equal(channel.ackCalled, true);
    assert.equal(channel.nackCalled, false);
    assert.deepEqual(sentPayload, {
        from: '+5511000000000',
        to: '+5511988887777',
        body: 'Oi'
    });

    if (originalFrom === undefined) {
        delete process.env.SMS_FROM_PHONE;
    } else {
        process.env.SMS_FROM_PHONE = originalFrom;
    }
});

test('handleQueueMessage envia nack para payload invalido', async () => {
    let createCallCount = 0;
    const smsClient = {
        messages: {
            create: async () => {
                createCallCount += 1;
                return { sid: 'SM456' };
            }
        }
    };

    const channel = {
        ackCalled: false,
        nackArgs: null,
        ack() {
            this.ackCalled = true;
        },
        nack(msg, multiple, requeue) {
            this.nackArgs = { msg, multiple, requeue };
        }
    };

    const message = {
        content: Buffer.from(
            JSON.stringify({
                phone: '(11) 98888-7777'
            })
        )
    };

    await handleQueueMessage(channel, smsClient, message);

    assert.equal(channel.ackCalled, false);
    assert.equal(createCallCount, 0);
    assert.deepEqual(channel.nackArgs, {
        msg: message,
        multiple: false,
        requeue: true
    });
});

test('createSmsClient falha sem variaveis do Twilio', () => {
    const original = {
        TWILIO_ACCOUNT_SID: process.env.TWILIO_ACCOUNT_SID,
        TWILIO_AUTH_TOKEN: process.env.TWILIO_AUTH_TOKEN,
        SMS_FROM_PHONE: process.env.SMS_FROM_PHONE
    };

    delete process.env.TWILIO_ACCOUNT_SID;
    delete process.env.TWILIO_AUTH_TOKEN;
    delete process.env.SMS_FROM_PHONE;

    assert.throws(
        () => createSmsClient(),
        /TWILIO_ACCOUNT_SID|TWILIO_AUTH_TOKEN|SMS_FROM_PHONE/
    );

    if (original.TWILIO_ACCOUNT_SID === undefined) {
        delete process.env.TWILIO_ACCOUNT_SID;
    } else {
        process.env.TWILIO_ACCOUNT_SID = original.TWILIO_ACCOUNT_SID;
    }

    if (original.TWILIO_AUTH_TOKEN === undefined) {
        delete process.env.TWILIO_AUTH_TOKEN;
    } else {
        process.env.TWILIO_AUTH_TOKEN = original.TWILIO_AUTH_TOKEN;
    }

    if (original.SMS_FROM_PHONE === undefined) {
        delete process.env.SMS_FROM_PHONE;
    } else {
        process.env.SMS_FROM_PHONE = original.SMS_FROM_PHONE;
    }
});

test('createSmsClient carrega variaveis do .env em processo limpo', () => {
    const childEnv = { ...process.env };
    delete childEnv.TWILIO_ACCOUNT_SID;
    delete childEnv.TWILIO_AUTH_TOKEN;
    delete childEnv.SMS_FROM_PHONE;

    const script = [
        "const { createSmsClient } = require('./dist/consumer_send_sms');",
        'createSmsClient();',
        "console.log('ok');"
    ].join('');

    const result = spawnSync('node', ['-e', script], {
        cwd: path.resolve(__dirname),
        env: childEnv,
        encoding: 'utf8'
    });

    assert.equal(
        result.status,
        0,
        `Processo filho falhou.\nstdout: ${result.stdout}\nstderr: ${result.stderr}`
    );
    assert.match(result.stdout, /ok/);
});
