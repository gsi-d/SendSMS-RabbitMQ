#!/usr/bin/env node
"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.normalizePhoneNumber = normalizePhoneNumber;
exports.normalizeMobileMessageRequest = normalizeMobileMessageRequest;
exports.createSmsClient = createSmsClient;
exports.sendSms = sendSms;
exports.handleQueueMessage = handleQueueMessage;
exports.main = main;
require("dotenv/config");
const amqplib_1 = __importDefault(require("amqplib"));
const twilio_1 = __importDefault(require("twilio"));
const DEFAULT_RABBITMQ_URL = 'amqp://localhost';
const DEFAULT_QUEUE_NAME = 'mobile_messages_queue';
function isRecord(value) {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}
function normalizePhoneNumber(phone) {
    if (typeof phone !== 'string') {
        return '';
    }
    const trimmed = phone.trim();
    if (!trimmed) {
        return '';
    }
    const digitsOnly = trimmed.replace(/\D/g, '');
    if (!digitsOnly) {
        return '';
    }
    if (trimmed.startsWith('+') && digitsOnly.length >= 10 && digitsOnly.length <= 15) {
        return `+${digitsOnly}`;
    }
    if (digitsOnly.startsWith('55') && digitsOnly.length >= 12 && digitsOnly.length <= 13) {
        return `+${digitsOnly}`;
    }
    if (digitsOnly.length === 10 || digitsOnly.length === 11) {
        return `+55${digitsOnly}`;
    }
    return '';
}
function normalizeMobileMessageRequest(request) {
    if (!isRecord(request)) {
        throw new Error('Payload invalido: esperado objeto JSON.');
    }
    const normalizedPhone = normalizePhoneNumber(request.phone);
    const message = typeof request.message === 'string' ? request.message.trim() : '';
    if (!normalizedPhone) {
        throw new Error('Payload invalido: campo "phone" obrigatorio ou invalido.');
    }
    if (!message) {
        throw new Error('Payload invalido: campo "message" obrigatorio.');
    }
    return {
        phone: normalizedPhone,
        message
    };
}
function createSmsClient() {
    const accountSid = process.env.TWILIO_ACCOUNT_SID;
    const authToken = process.env.TWILIO_AUTH_TOKEN;
    const fromPhone = process.env.SMS_FROM_PHONE;
    if (!accountSid || !authToken || !fromPhone) {
        throw new Error('Defina TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN e SMS_FROM_PHONE para envio real de SMS.');
    }
    return (0, twilio_1.default)(accountSid, authToken);
}
async function sendSms(smsClient, payload) {
    const fromPhone = process.env.SMS_FROM_PHONE;
    return smsClient.messages.create({
        from: fromPhone,
        to: payload.phone,
        body: payload.message
    });
}
async function handleQueueMessage(channel, smsClient, msg) {
    if (!msg) {
        return;
    }
    try {
        const content = msg.content.toString();
        const request = JSON.parse(content);
        const normalized = normalizeMobileMessageRequest(request);
        console.log(' [x] Processando mensagem para celular:', normalized);
        const response = await sendSms(smsClient, normalized);
        console.log(` [v] SMS enviado para ${normalized.phone}. SID: ${response.sid || 'indisponivel'}`);
        channel.ack(msg);
    }
    catch (error) {
        console.error(' [!] Erro ao processar mensagem:', error);
        channel.nack(msg, false, true);
    }
}
async function main() {
    const rabbitmqUrl = process.env.RABBITMQ_URL || DEFAULT_RABBITMQ_URL;
    const queue = process.env.MOBILE_QUEUE_NAME || DEFAULT_QUEUE_NAME;
    const smsClient = createSmsClient();
    const connection = await amqplib_1.default.connect(rabbitmqUrl);
    const channel = await connection.createChannel();
    await channel.assertQueue(queue, {
        durable: true,
        arguments: { 'x-queue-type': 'quorum' }
    });
    channel.prefetch(1);
    console.log(` [*] Aguardando mensagens de celular na fila "${queue}"...`);
    channel.consume(queue, (msg) => {
        void handleQueueMessage(channel, smsClient, msg).catch((error) => {
            console.error(' [!] Falha nao tratada no consumo da fila:', error);
            if (msg) {
                channel.nack(msg, false, true);
            }
        });
    });
}
if (require.main === module) {
    void main().catch((err) => {
        console.error('Erro no consumer:', err);
        process.exit(1);
    });
}
