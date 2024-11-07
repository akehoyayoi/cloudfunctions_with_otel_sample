/**
 * Import function triggers from their respective submodules:
 *
 * import {onCall} from "firebase-functions/v2/https";
 * import {onDocumentWritten} from "firebase-functions/v2/firestore";
 *
 * See a full list of supported triggers at https://firebase.google.com/docs/functions
 */

import {onRequest} from "firebase-functions/v2/https";
import * as logger from "firebase-functions/logger";
import {onMessagePublished} from "firebase-functions/v2/pubsub";
import {initializeApp} from "firebase-admin/app";
import {PubSub} from "@google-cloud/pubsub";
import { context, propagation, trace } from '@opentelemetry/api';
import * as opentelemetry from "@opentelemetry/sdk-node";
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { Resource } from '@opentelemetry/resources';
import { ATTR_SERVICE_NAME } from '@opentelemetry/semantic-conventions';
import { TraceExporter } from '@google-cloud/opentelemetry-cloud-trace-exporter';
import {
  BatchSpanProcessor,
 } from "@opentelemetry/sdk-trace-base";

// OpenTelemetryの初期化関数
function initializeOpenTelemetry() {
  const traceExporter = new TraceExporter();
  const spanProcessor = new BatchSpanProcessor(traceExporter);
  const sdk = new opentelemetry.NodeSDK({
    resource: new Resource({
      [ATTR_SERVICE_NAME]: 'firebase-functions',
    }),
    traceExporter,
    spanProcessor,
    instrumentations: [getNodeAutoInstrumentations()],
  });

  sdk.start();

  // プロセス終了時にSDKをシャットダウン
  process.on('SIGTERM', () => {
    sdk.shutdown()
      .then(() => logger.info('OpenTelemetry SDK shut down'))
      .catch((error) => logger.error('Error shutting down OpenTelemetry SDK', error))
      .finally(() => process.exit(0));
  });
}

// Firebase Adminの初期化
initializeApp();

// OpenTelemetryの初期化
initializeOpenTelemetry();

// PubSubクライアントの初期化
const pubSubClient = new PubSub();

export const helloWorld = onRequest(
  { 
    region: "asia-northeast1",  // 東京リージョン
    cors: true,
  },
  async (request, response) => {
    console.log("headers", request.headers);
    
    // リクエストヘッダーからコンテキストを抽出
    const parentContext = propagation.extract(context.active(), request.headers);
    if(!parentContext) {
      throw new Error("No parent context found");
    }

    try {
      return context.with(parentContext, async () => {
        // testトピックにメッセージを送信
        const messageData = {
          message: "Hello from Firebase!",
          timestamp: new Date().toISOString()
        };

        const attributes: {[key: string]: string} = {};

        // OpenTelemetryのコンテキストを属性に注入
        propagation.inject(context.active(), attributes);
        logger.info("attributes", {attributes});
        const topic = pubSubClient.topic("test");
        
        await new Promise(resolve => setTimeout(resolve, 500));
        
        const messageId = await topic.publishMessage({
          data: Buffer.from(JSON.stringify(messageData)),
          attributes: attributes,
        });

        await new Promise(resolve => setTimeout(resolve, 500));

        logger.info("Message published", {messageId});
        response.send(`Message published successfully! Message ID: ${messageId}`);
      });
    } catch (error) {
      logger.error("Error publishing message", error);
      response.status(500).send("Error publishing message");
    }
});

export const handleTestMessage = onMessagePublished(
  {
    topic: "test",
    region: "asia-northeast1"
  },
  async (event) => {
    const tracer = trace.getTracer('firebase-functions');
    // OpenTelemetryのコンテキストを属性から抽出
    let attributes = event.data.message.attributes;
    if (attributes) {
      const parentContext = propagation.extract(context.active(), attributes);
      return context.with(parentContext, async() => {
        const span = tracer.startSpan('handleTestMessage');
        try {
          // メッセージのデータを取得
          const message = event.data.message.json;
          
          // ログ出力
          logger.info("Received message on 'test' topic", message);
          await new Promise(resolve => setTimeout(resolve, 1000));

        } finally {
          span.end();
        }
      });
    } else {
        // 属性がない��合は通常の処理を実行
        const message = event.data.message.json;
        logger.info("Received message on 'test' topic", message);
    }    
  }
);
