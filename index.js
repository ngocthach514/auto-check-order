require("dotenv").config();
const axios = require("axios");
const axiosRetry = require("axios-retry").default;
const { Sequelize, DataTypes, Op } = require("sequelize");
const { default: PQueue } = require("p-queue");
const stringSimilarity = require("string-similarity");
const winston = require("winston");
const fs = require("fs").promises;
const path = require("path");
const crypto = require("crypto");
const NodeCache = require("node-cache");

// Biến toàn cục để theo dõi tổng số token và cache hit/miss
let totalTokensUsed = 0;
let cacheHits = 0;
let cacheMisses = 0;

// Cấu hình logging
const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => `${timestamp} [${level.toUpperCase()}]: ${message}`)
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: "app.log" }),
  ],
});

// Cấu hình từ biến môi trường
const CONFIG = {
  SIMILARITY_THRESHOLD: parseFloat(process.env.SIMILARITY_THRESHOLD || "0.8"),
  KEYWORD_MATCH_THRESHOLD: parseFloat(process.env.KEYWORD_MATCH_THRESHOLD || "0.7"),
  EXACT_MATCH_THRESHOLD: parseFloat(process.env.EXACT_MATCH_THRESHOLD || "0.95"),
  GROUP_SIMILARITY_THRESHOLD: parseFloat(process.env.GROUP_SIMILARITY_THRESHOLD || "0.8"),
  BATCH_LIMIT: parseInt(process.env.BATCH_LIMIT || "10000"),
  API_RETRIES: parseInt(process.env.API_RETRIES || "3"),
  QUEUE_CONCURRENCY: parseInt(process.env.QUEUE_CONCURRENCY || "10"),
  MAX_CONVERSATION_IDS_PER_BATCH: parseInt(process.env.MAX_CONVERSATION_IDS_PER_BATCH || "20"),
  MAX_TOKEN_PER_BATCH: parseInt(process.env.MAX_TOKEN_PER_BATCH || "50000"),
  MAX_CACHE_SIZE: parseInt(process.env.MAX_CACHE_SIZE || "10000"),
  CACHE_TTL: parseInt(process.env.CACHE_TTL || "604800"), // 7 ngày (7 * 24 * 60 * 60)
};

// Thư mục lưu file JSON
const JSON_LOG_DIR = path.join(__dirname, "logs", "json");

// Tạo thư mục logs/json nếu chưa tồn tại
async function ensureJsonLogDir() {
  try {
    await fs.mkdir(JSON_LOG_DIR, { recursive: true });
  } catch (error) {
    logger.error(`Lỗi khi tạo thư mục logs/json: ${error.message}`);
  }
}

// Khởi tạo cache với TTL
const CACHE_FILE = "cache.json";
const memoryCache = new NodeCache({ stdTTL: CONFIG.CACHE_TTL, checkperiod: 3600 });
let diskCache = {};
const cacheAccessCount = new Map(); // Theo dõi tần suất truy cập cache

async function loadCache() {
  try {
    const data = await fs.readFile(CACHE_FILE, "utf8");
    diskCache = JSON.parse(data);
    for (const [key, value] of Object.entries(diskCache)) {
      memoryCache.set(key, value);
    }
    logger.info(`Đã tải cache từ file, số mục: ${Object.keys(diskCache).length}`);
  } catch (error) {
    diskCache = {};
    logger.info("Không tìm thấy cache, khởi tạo mới");
  }
}

async function saveCache() {
  try {
    logger.info(`Chuẩn bị lưu cache, số mục: ${Object.keys(diskCache).length}`);
    if (Object.keys(diskCache).length > CONFIG.MAX_CACHE_SIZE) {
      const sortedKeys = Object.keys(diskCache).sort((a, b) => {
        const accessA = cacheAccessCount.get(a) || 0;
        const accessB = cacheAccessCount.get(b) || 0;
        return accessB - accessA; // Sắp xếp theo tần suất truy cập giảm dần
      });
      const keysToDelete = sortedKeys.slice(CONFIG.MAX_CACHE_SIZE / 2);
      for (const key of keysToDelete) {
        delete diskCache[key];
        memoryCache.del(key);
        cacheAccessCount.delete(key);
      }
      logger.info(`Đã xóa ${keysToDelete.length} mục cache ít truy cập nhất`);
    }
    await fs.writeFile(CACHE_FILE, JSON.stringify(diskCache, null, 2));
    logger.info("Đã lưu cache vào file");
  } catch (error) {
    logger.error(`Lỗi lưu cache: ${error.message}`);
  }
}

// Khởi tạo Sequelize
const sequelize = new Sequelize("nguyenkim-autozalo", "root", "", {
  host: "localhost",
  dialect: "mysql",
  port: 3306,
  logging: false,
});

// Định nghĩa model Order
const Order = sequelize.define("orders", {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  conversation_id: DataTypes.INTEGER,
  item: DataTypes.STRING,
  status: DataTypes.STRING,
  deleted_at: DataTypes.DATE,
  created_at: DataTypes.DATE,
}, { timestamps: false });

// Cấu hình axios-retry
axiosRetry(axios, {
  retries: CONFIG.API_RETRIES,
  retryDelay: (retryCount) => {
    const delay = Math.pow(2, retryCount) * 1000;
    logger.warn(`Retry ${retryCount}: Chờ ${delay}ms...`);
    return delay;
  },
  retryCondition: (error) => (
    axiosRetry.isNetworkOrIdempotentRequestError(error) ||
    (error.response && error.response.status === 429) ||
    error.code === 'ECONNABORTED'
  ),
});

// Cấu hình hàng đợi
const queues = Array.from(
  { length: 20 },
  () => new PQueue({ concurrency: CONFIG.QUEUE_CONCURRENCY, interval: 500, intervalCap: 10 })
);

// Ước lượng token
function estimateTokens(str) {
  return Math.ceil(str.length / 4);
}

// Tạo hash cho chuỗi
function hashItem(item) {
  return crypto.createHash("md5").update(item.toLowerCase()).digest("hex");
}

// Tạo hash cho nhóm
function hashGroup(group) {
  const items = group.map((order) => order.item).sort().join("|");
  return crypto.createHash("md5").update(items.toLowerCase()).digest("hex");
}

// Trích xuất từ khóa
function extractKeyWords(item) {
  const commonWords = ["and", "for", "with", "bit", "dvd", "oei", "dsp", "intl"];
  return item
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, "")
    .split(/\s+/)
    .filter((word) => word.length > 2 && !commonWords.includes(word))
    .map((word) => word.trim());
}

// Kiểm tra độ tương đồng
function isSimilar(item1, item2, exactMatch = false) {
  const simScore = stringSimilarity.compareTwoStrings(item1.toLowerCase(), item2.toLowerCase());
  if (exactMatch && simScore > CONFIG.EXACT_MATCH_THRESHOLD) return true;

  const keywords1 = extractKeyWords(item1);
  const keywords2 = extractKeyWords(item2);
  const commonKeywords = keywords1.filter((kw) => keywords2.includes(kw));
  const keywordMatchRatio = commonKeywords.length / Math.max(keywords1.length, keywords2.length, 1);

  const dynamicThreshold = Math.min(CONFIG.SIMILARITY_THRESHOLD, 0.9 - 0.1 * (item1.length / 50));
  return (
    simScore > dynamicThreshold ||
    (keywordMatchRatio > CONFIG.KEYWORD_MATCH_THRESHOLD && simScore > dynamicThreshold * 0.8)
  );
}

// Kiểm tra tương đồng nhóm
function isGroupSimilar(newItems, cachedItems) {
  const matchedItems = newItems.filter(item1 =>
    cachedItems.some(item2 => isSimilar(item1, item2, false))
  );
  const similarityRatio = matchedItems.length / Math.max(newItems.length, cachedItems.length);
  return similarityRatio > CONFIG.GROUP_SIMILARITY_THRESHOLD;
}

// Nhóm các Item tương đồng
function groupSimilarItems(orders) {
  const groups = [];
  const visited = new Set();
  for (let i = 0; i < orders.length; i++) {
    if (visited.has(i)) continue;
    const group = [orders[i]];
    visited.add(i);
    for (let j = i + 1; j < orders.length; j++) {
      if (!visited.has(j) && isSimilar(orders[i].item, orders[j].item)) {
        group.push(orders[j]);
        visited.add(j);
      }
    }
    groups.push(group);
  }
  return groups;
}

// Xử lý trùng lặp cục bộ
function processLocalDuplicates(groups) {
  return groups.map((group) => {
    if (group.length <= 1) return group;
    const uniqueItems = [];
    const seenHashes = new Set();
    for (const order of group) {
      const itemHash = hashItem(order.item);
      if (isSimilar(order.item, group[0].item, true) || seenHashes.has(itemHash)) {
        if (!seenHashes.has(itemHash)) {
          uniqueItems.push(order);
          seenHashes.add(itemHash);
        }
      } else {
        uniqueItems.push(order);
        seenHashes.add(itemHash);
      }
    }
    return uniqueItems;
  });
}

// Định dạng chuỗi cho batch
function formatOrdersStringForBatch(batch) {
  return Object.entries(batch)
    .map(([conversationId, groups]) => {
      const groupStrings = groups
        .map((group, index) => {
          const groupItems = group
            .map((order) => `ID: ${order.id}, Conversation ID: ${order.conversation_id}, Item: ${order.item}`)
            .join("\n");
          return `G:${index + 1}:\n${groupItems}`;
        })
        .join("\n--\n");
      return `C:${conversationId}\n${groupStrings}`;
    })
    .join("\n==\n");
}

// Gọi API OpenAI
async function callOpenAIApi(batchConversationIds, ordersString) {
  const systemMessage = `- Bạn là một AI chuyên phân tích và xử lý từ ngữ trong lĩnh vực công nghệ và đồ điện tử.
- Các sản phẩm được đề cập liên quan đến phần mềm, phần cứng và các thiết bị điện tử đa dụng. Hãy tập trung vào việc so sánh các từ khóa chính, mẫu mã sản phẩm, phiên bản, ngôn ngữ, và các đặc điểm kỹ thuật để đưa ra nhận định chính xác nhất.

**NHIỆM VỤ**
- Phân tích các chuỗi ký tự trong trường "Item" của các bản ghi dưới đây để xác định xem chúng có **CÙNG LÀ MỘT SẢN PHẨM HAY KHÔNG**.

**HƯỚNG DẪN PHÂN TÍCH**
- Xác định từ khóa chính: Tìm các từ khóa liên quan đến sản phẩm.
- Phân loại các từ khóa theo: Tên sản phẩm, phiên bản, định dạng phân phối, và các tiêu chí đánh giá thông tin sản phẩm khác.
- So Sánh Các Đặc Điểm: So sánh tên sản phẩm chính.
Kiểm tra xem các đặc điểm bổ sung (ngôn ngữ, định dạng, mã sản phẩm) có tương thích hay chỉ ra sự khác biệt.
Xác định xem các thuật ngữ có ám chỉ cùng một loại sản phẩm hay các biến thể khác nhau.

**VÍ DỤ CỤ THỨC**
- Đối với các sản phẩm Dell OptiPlex:
  - "7020MT" là Mini Tower, trong khi "7020 SFF" là Small Form Factor, đây là hai form factor khác nhau.
  - CPU như "Core i3-12100" và "Core i3-14100" là các thế hệ khác nhau với hiệu suất khác nhau.
  - Hệ điều hành như "Ubuntu" và "Windows 11 Home" là khác nhau.

**KẾT LUẬN MONG MUỐN**
Dựa trên sự tương đồng hoặc khác biệt của các từ khóa và đặc điểm, đưa ra nhận định liệu các "Item" này có cùng chỉ một sản phẩm hay không.

**LƯU Ý QUAN TRỌNG**
- Hãy xử lí rồi trả kết quả là chuỗi JSON như dữ liệu đầu ra mong muốn phía dưới, không cần giải thích phân tích hoặc kết luận.
- Trả về danh sách các sản phẩm không trùng lặp, giữ lại sản phẩm có mô tả đầy đủ nhất cho mỗi nhóm trùng lặp.

**CẤU TRÚC DỮ LIỆU ĐẦU VÀO**
C:<conversation_id>
G:<group_number>:
ID: <id>, Conversation ID: <conversation_id>, Item: <item>
...
--
G:<group_number>:
...
==
C:<conversation_id>
...

Ví dụ:
C:4085
G:1:
ID: 22305, Conversation ID: 4085, Item: Win Pro 11 64Bit Eng Intl 1pk DSP OEI DVD (FQC-10528)
ID: 22306, Conversation ID: 4085, Item: DG7GMGF0L4TL Windows fos - Windows 11 Pro - Legalization Get Genuine
ID: 22307, Conversation ID: 4085, Item: Windows 11 Pro - Legalization Get Genuine
--
G:2:
ID: 22308, Conversation ID: 4085, Item: MÁY IN HP PRO MFP 4103FDN ( 2Z628A )
ID: 22309, Conversation ID: 4085, Item: Printer
==
C:4086
G:1:
ID: 22310, Conversation ID: 4086, Item: Dell OptiPlex 7020MT

**DỮ LIỆU ĐẦU RA MONG MUỐN**
[
  {
    "conversation_id": 4085,
    "groups": [
      {
        "group": 1,
        "uniqueItems": [
          {
            "ID": 22305,
            "Conversation ID": 4085,
            "Item": "Win Pro 11 64Bit Eng Intl 1pk DSP OEI DVD (FQC-10528)"
          },
          {
            "ID": 22306,
            "Conversation ID": 4085,
            "Item": "DG7GMGF0L4TL Windows GGWA - Windows 11 Pro - Legalization Get Genuine"
          }
        ]
      },
      {
        "group": 2,
        "uniqueItems": [
          {
            "ID": 22308,
            "Conversation ID": 4085,
            "Item": "MÁY IN HP PRO MFP 4103FDN ( 2Z628A )"
          }
        ]
      }
    ]
  },
  {
    "conversation_id": 4086,
    "groups": [
      {
        "group": 1,
        "uniqueItems": [
          {
            "ID": 22310,
            "Conversation ID": 4086,
            "Item": "Dell OptiPlex 7020MT"
          }
        ]
      }
    ]
  }
]`;

  try {
    const tokenCount = estimateTokens(systemMessage + ordersString);
    logger.info(`Gửi batch ${batchConversationIds.length} conversation_id, ước lượng ${tokenCount} token`);

    const response = await axios.post(
      "https://api.openai.com/v1/chat/completions",
      {
        model: "gpt-4o-mini-2024-07-18",
        messages: [
          { role: "system", content: systemMessage },
          { role: "user", content: ordersString },
        ],
      },
      {
        headers: {
          Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
          "Content-Type": "application/json",
        },
        timeout: 120000,
      }
    );
    let content = response.data.choices[0].message.content || "{}";
    content = content.replace(/```json\n|\n```/g, "").trim();
    const usage = response.data.usage;
    totalTokensUsed += usage.total_tokens;
    logger.info(`API OpenAI: ${batchConversationIds.join(", ")}, Token lần này: ${usage.total_tokens}, Tổng token đã tiêu hao: ${totalTokensUsed}`);
    await fs.writeFile(path.join(JSON_LOG_DIR, `debug_json_${batchConversationIds.join("_")}.json`), content);
    return content;
  } catch (error) {
    logger.error(`Lỗi API OpenAI: ${batchConversationIds.join(", ")}: ${error.message}`);
    throw error;
  }
}

// Xác thực JSON
function validateJsonStructure(json) {
  if (!Array.isArray(json)) return false;
  return json.every(
    (conv) =>
      typeof conv.conversation_id === "number" &&
      Array.isArray(conv.groups) &&
      conv.groups.every(
        (group) =>
          typeof group.group === "number" &&
          Array.isArray(group.uniqueItems) &&
          group.uniqueItems.every(
            (item) =>
              typeof item.ID === "number" &&
              typeof item["Conversation ID"] === "number" &&
              typeof item.Item === "string"
          )
      )
  );
}

// Phân tích JSON
async function parseJsonSafely(content, batchConversationIds, batch, groupCacheHits) {
  try {
    const parsed = JSON.parse(content);
    if (!validateJsonStructure(parsed)) {
      logger.warn(`JSON không đúng định dạng: ${batchConversationIds.join(", ")}`);
      const fallbackResult = Object.entries(batch).map(([conversationId, groups]) => ({
        conversation_id: parseInt(conversationId),
        groups: groups.map((group, index) => ({
          group: index + 1,
          uniqueItems: groupCacheHits[conversationId]?.[index] || group,
        })),
      }));
      fallbackResult.forEach((convResult) => {
        const conversationId = convResult.conversation_id.toString();
        convResult.groups.forEach((groupResult, index) => {
          const group = batch[conversationId][index];
          const groupKey = hashGroup(group);
          diskCache[groupKey] = {
            uniqueItems: groupResult.uniqueItems,
            originalItems: group.map(o => o.item),
          };
          memoryCache.set(groupKey, {
            uniqueItems: groupResult.uniqueItems,
            originalItems: group.map(o => o.item),
          });
          logger.info(`Lưu cache mặc định cho groupKey: ${groupKey}, conversation_id: ${conversationId}`);
        });
      });
      return fallbackResult;
    }

    parsed.forEach((convResult) => {
      const conversationId = convResult.conversation_id.toString();
      convResult.groups.forEach((groupResult, index) => {
        const group = batch[conversationId][index];
        const groupKey = hashGroup(group);
        diskCache[groupKey] = {
          uniqueItems: groupResult.uniqueItems,
          originalItems: group.map(o => o.item),
        };
        memoryCache.set(groupKey, {
          uniqueItems: groupResult.uniqueItems,
          originalItems: group.map(o => o.item),
        });
        logger.info(`Lưu cache cho groupKey: ${groupKey}, conversation_id: ${conversationId}`);
      });
    });

    return parsed;
  } catch (error) {
    logger.error(`Lỗi phân tích JSON: ${batchConversationIds.join(", ")}: ${error.message}`);
    await fs.writeFile(path.join(JSON_LOG_DIR, `error_json_${batchConversationIds.join("_")}.json`), content);
    const fallbackResult = Object.entries(batch).map(([conversationId, groups]) => ({
      conversation_id: parseInt(conversationId),
      groups: groups.map((group, index) => ({
        group: index + 1,
        uniqueItems: groupCacheHits[conversationId]?.[index] || group,
      })),
    }));
    fallbackResult.forEach((convResult) => {
      const conversationId = convResult.conversation_id.toString();
      convResult.groups.forEach((groupResult, index) => {
        const group = batch[conversationId][index];
        const groupKey = hashGroup(group);
        diskCache[groupKey] = {
          uniqueItems: groupResult.uniqueItems,
          originalItems: group.map(o => o.item),
        };
        memoryCache.set(groupKey, {
          uniqueItems: groupResult.uniqueItems,
          originalItems: group.map(o => o.item),
        });
        logger.info(`Lưu cache mặc định cho groupKey: ${groupKey}, conversation_id: ${conversationId}`);
      });
    });
    return fallbackResult;
  }
}

// Lấy dữ liệu từ database
async function fetchOrdersBatch(offset = 0, limit = CONFIG.BATCH_LIMIT) {
  logger.info(`Lấy dữ liệu từ offset ${offset}, limit ${limit}...`);
  try {
    const threeDaysAgo = new Date();
    threeDaysAgo.setDate(threeDaysAgo.getDate() - 3);
    threeDaysAgo.setHours(0, 0, 0, 0);
    const now = new Date();

    logger.info(`Lấy dữ liệu từ ${threeDaysAgo} đến ${now}`);

    const orders = await sequelize.query(
      `
      SELECT 
        id, 
        conversation_id, 
        item, 
        status, 
        deleted_at, 
        created_at
      FROM 
        orders
      WHERE 
        status = 'pending' 
        AND deleted_at IS NULL 
        AND created_at >= :threeDaysAgo
        AND created_at <= :now
        AND conversation_id IN (
          SELECT conversation_id 
          FROM orders 
          WHERE 
            status = 'pending' 
            AND deleted_at IS NULL 
            AND created_at >= :threeDaysAgo
            AND created_at <= :now
          GROUP BY conversation_id 
          HAVING COUNT(*) >= 2
        )
      ORDER BY 
        conversation_id ASC
      LIMIT :limit OFFSET :offset
      `,
      {
        replacements: { threeDaysAgo, now, limit, offset },
        type: Sequelize.QueryTypes.SELECT,
      }
    );

    logger.info(`Lấy được ${orders.length} bản ghi`);
    if (orders.length === 0) {
      const totalRecords = await sequelize.query(
        `SELECT COUNT(*) as count FROM orders`,
        { type: Sequelize.QueryTypes.SELECT }
      );
      const pendingRecords = await sequelize.query(
        `SELECT COUNT(*) as count FROM orders WHERE status = 'pending' AND deleted_at IS NULL`,
        { type: Sequelize.QueryTypes.SELECT }
      );
      const dateRangeRecords = await sequelize.query(
        `SELECT COUNT(*) as count FROM orders WHERE created_at >= :threeDaysAgo AND created_at <= :now`,
        { replacements: { threeDaysAgo, now }, type: Sequelize.QueryTypes.SELECT }
      );
      logger.info(`Tổng số bản ghi trong bảng: ${totalRecords[0].count}`);
      logger.info(`Số bản ghi pending và chưa xóa: ${pendingRecords[0].count}`);
      logger.info(`Số bản ghi trong khoảng ${threeDaysAgo} đến ${now}: ${dateRangeRecords[0].count}`);
    }
    return orders;
  } catch (error) {
    logger.error(`Lỗi lấy dữ liệu: ${error.message}`);
    return [];
  }
}

// Xử lý batch
async function processConversationBatch(batch) {
  const startTime = Date.now();
  const batchConversationIds = Object.keys(batch);
  logger.info(`Bắt đầu batch ${batchConversationIds.join(", ")}`);

  const groupCacheHits = {};
  const groupsToProcess = {};
  let allCached = true;

  for (const [conversationId, groups] of Object.entries(batch)) {
    groupCacheHits[conversationId] = [];
    groupsToProcess[conversationId] = [];
    groups.forEach((group, index) => {
      const groupKey = hashGroup(group);
      let cachedResult = memoryCache.get(groupKey);
      const groupItems = group.map(o => o.item).join(", ");
      if (cachedResult) {
        cacheHits++;
        cacheAccessCount.set(groupKey, (cacheAccessCount.get(groupKey) || 0) + 1);
        logger.info(`Cache hit cho groupKey: ${groupKey}, conversation_id: ${conversationId}, group: ${index + 1}`);
        logger.info(`Nhóm trùng với lần chạy trước, không gọi API: conversation_id: ${conversationId}, group: ${index + 1}, Items: [${groupItems}]`);
        groupCacheHits[conversationId][index] = cachedResult.uniqueItems;
      } else {
        // Kiểm tra tương đồng nhóm
        const newItems = group.map(o => o.item).sort();
        let foundSimilar = false;
        const cacheKeys = memoryCache.keys();
        for (const key of cacheKeys) {
          const value = memoryCache.get(key);
          if (value && isGroupSimilar(newItems, value.originalItems)) {
            cacheHits++;
            cacheAccessCount.set(key, (cacheAccessCount.get(key) || 0) + 1);
            cachedResult = value.uniqueItems;
            logger.info(`Cache hit cho nhóm tương đồng với key: ${key}, conversation_id: ${conversationId}, group: ${index + 1}`);
            logger.info(`Nhóm tương đồng với lần chạy trước, không gọi API: conversation_id: ${conversationId}, group: ${index + 1}, Items: [${groupItems}]`);
            groupCacheHits[conversationId][index] = cachedResult;
            foundSimilar = true;
            break;
          }
        }
        if (!foundSimilar) {
          cacheMisses++;
          logger.info(`Cache miss cho groupKey: ${groupKey}, conversation_id: ${conversationId}, group: ${index + 1}`);
          logger.info(`Nhóm có dữ liệu mới, cần gọi API: conversation_id: ${conversationId}, group: ${index + 1}, Items: [${groupItems}]`);
          groupsToProcess[conversationId].push(group);
          allCached = false;
        }
      }
    });
    if (groupsToProcess[conversationId].length === 0) {
      delete groupsToProcess[conversationId];
    }
  }

  if (allCached) {
    logger.info(`Sử dụng cache hoàn toàn cho batch ${batchConversationIds.join(", ")}`);
    return Object.entries(batch).map(([conversationId, groups]) => ({
      conversation_id: parseInt(conversationId),
      groups: groups.map((group, index) => ({
        group: index + 1,
        uniqueItems: groupCacheHits[conversationId][index],
      })),
    }));
  }

  const ordersString = formatOrdersStringForBatch(groupsToProcess);
  if (!ordersString) {
    logger.info(`Không có dữ liệu mới để xử lý: ${batchConversationIds.join(", ")}`);
    return Object.entries(batch).map(([conversationId, groups]) => ({
      conversation_id: parseInt(conversationId),
      groups: groups.map((group, index) => ({
        group: index + 1,
        uniqueItems: groupCacheHits[conversationId][index] || group,
      })),
    }));
  }

  try {
    const content = await callOpenAIApi(batchConversationIds, ordersString);
    const result = parseJsonSafely(content, batchConversationIds, groupsToProcess, groupCacheHits);

    const transaction = await sequelize.transaction();
    try {
      for (const convResult of result) {
        const conversationId = convResult.conversation_id.toString();
        const groupsWithDuplicates = batch[conversationId];
        const tableData = [];
        let totalDeleted = 0;

        for (const groupResult of convResult.groups) {
          const keptOrderIds = groupResult.uniqueItems.map((order) => order.ID);
          const groupIndex = groupResult.group - 1;
          const originalGroup = groupsWithDuplicates[groupIndex];

          const idsToDelete = originalGroup
            .map((order) => order.id)
            .filter((id) => !keptOrderIds.includes(id));

          totalDeleted += idsToDelete.length;

          for (const id of idsToDelete) {
            const order = originalGroup.find((o) => o.id === id);
            tableData.push({
              Conversation_ID: conversationId,
              Group: groupResult.group,
              Item_Deleted: order.item,
              Item_Kept: "Không có",
            });
          }

          for (const keptOrder of groupResult.uniqueItems) {
            tableData.push({
              Conversation_ID: conversationId,
              Group: groupResult.group,
              Item_Deleted: "Không có",
              Item_Kept: keptOrder.Item,
            });
          }
        }

        if (tableData.length) {
          logger.info(`\nKết quả cho Conversation ID: ${conversationId}`);
          logger.info("-".repeat(120));
          console.table(tableData, ["Conversation_ID", "Group", "Item_Deleted", "Item_Kept"]);
          logger.info(`Đã xóa ${totalDeleted} bản ghi trùng lặp cho Conversation ID: ${conversationId}`);

          const allIdsToDelete = tableData
            .filter((row) => row.Item_Deleted !== "Không có")
            .map((row) => {
              const order = batch[conversationId].flat().find((o) => o.item === row.Item_Deleted);
              return order.id;
            });

          if (allIdsToDelete.length) {
            await Order.update(
              { deleted_at: new Date() },
              { where: { id: allIdsToDelete }, transaction }
            );
          }
        }
      }

      await transaction.commit();
      await saveCache();
    } catch (error) {
      await transaction.rollback();
      logger.error(`Lỗi cập nhật database: ${batchConversationIds.join(", ")}: ${error.message}`);
      throw error;
    }
    return result;
  } catch (error) {
    logger.error(`Lỗi batch ${batchConversationIds.join(", ")}: ${error.message}`);
    for (const [conversationId, groups] of Object.entries(batch)) {
      await processConversationGroup(conversationId, groups.flat());
    }
  } finally {
    const runTime = (Date.now() - startTime) / 1000;
    logger.info(`Kết thúc batch ${batchConversationIds.join(", ")}. Thời gian: ${runTime} giây`);
  }
}

// Xử lý từng conversation_id
async function processConversationGroup(conversationId, orders) {
  const startTime = Date.now();
  logger.info(`Bắt đầu Conversation ID: ${conversationId}`);

  const groups = processLocalDuplicates(groupSimilarItems(orders));
  const groupsWithDuplicates = groups.filter((group) => group.length > 1);

  if (groupsWithDuplicates.length === 0) {
    logger.info(`Không có nhóm trùng lặp: ${conversationId}`);
    return;
  }

  const groupCacheHits = [];
  const groupsToProcess = [];
  groupsWithDuplicates.forEach((group, index) => {
    const groupKey = hashGroup(group);
    let cachedResult = memoryCache.get(groupKey);
    const groupItems = group.map(o => o.item).join(", ");
    if (cachedResult) {
      cacheHits++;
      cacheAccessCount.set(groupKey, (cacheAccessCount.get(groupKey) || 0) + 1);
      logger.info(`Cache hit cho groupKey: ${groupKey}, conversation_id: ${conversationId}, group: ${index + 1}`);
      logger.info(`Nhóm trùng với lần chạy trước, không gọi API: conversation_id: ${conversationId}, group: ${index + 1}, Items: [${groupItems}]`);
      groupCacheHits[index] = cachedResult.uniqueItems;
    } else {
      const newItems = group.map(o => o.item).sort();
      let foundSimilar = false;
      const cacheKeys = memoryCache.keys();
      for (const key of cacheKeys) {
        const value = memoryCache.get(key);
        if (value && isGroupSimilar(newItems, value.originalItems)) {
          cacheHits++;
          cacheAccessCount.set(key, (cacheAccessCount.get(key) || 0) + 1);
          cachedResult = value.uniqueItems;
          logger.info(`Cache hit cho nhóm tương đồng với key: ${key}, conversation_id: ${conversationId}, group: ${index + 1}`);
          logger.info(`Nhóm tương đồng với lần chạy trước, không gọi API: conversation_id: ${conversationId}, group: ${index + 1}, Items: [${groupItems}]`);
          groupCacheHits[index] = cachedResult;
          foundSimilar = true;
          break;
        }
      }
      if (!foundSimilar) {
        cacheMisses++;
        logger.info(`Cache miss cho groupKey: ${groupKey}, conversation_id: ${conversationId}, group: ${index + 1}`);
        logger.info(`Nhóm có dữ liệu mới, cần gọi API: conversation_id: ${conversationId}, group: ${index + 1}, Items: [${groupItems}]`);
        groupsToProcess.push(group);
      }
    }
  });

  if (groupsToProcess.length === 0) {
    logger.info(`Sử dụng cache hoàn toàn cho Conversation ID: ${conversationId}`);
    const result = [{
      conversation_id: parseInt(conversationId),
      groups: groupsWithDuplicates.map((group, index) => ({
        group: index + 1,
        uniqueItems: groupCacheHits[index],
      })),
    }];
    const transaction = await sequelize.transaction();
    try {
      for (const convResult of result) {
        const tableData = [];
        let totalDeleted = 0;

        for (const groupResult of convResult.groups) {
          const keptOrderIds = groupResult.uniqueItems.map((order) => order.ID);
          const groupIndex = groupResult.group - 1;
          const originalGroup = groupsWithDuplicates[groupIndex];

          const idsToDelete = originalGroup
            .map((order) => order.id)
            .filter((id) => !keptOrderIds.includes(id));

          totalDeleted += idsToDelete.length;

          for (const id of idsToDelete) {
            const order = originalGroup.find((o) => o.id === id);
            tableData.push({
              Conversation_ID: conversationId,
              Group: groupResult.group,
              Item_Deleted: order.item,
              Item_Kept: "Không có",
            });
          }

          for (const keptOrder of groupResult.uniqueItems) {
            tableData.push({
              Conversation_ID: conversationId,
              Group: groupResult.group,
              Item_Deleted: "Không có",
              Item_Kept: keptOrder.Item,
            });
          }
        }

        if (tableData.length) {
          logger.info(`\nKết quả cho Conversation ID: ${conversationId}`);
          logger.info("-".repeat(120));
          console.table(tableData, ["Conversation_ID", "Group", "Item_Deleted", "Item_Kept"]);
          logger.info(`Đã xóa ${totalDeleted} bản ghi trùng lặp cho Conversation ID: ${conversationId}`);

          const allIdsToDelete = tableData
            .filter((row) => row.Item_Deleted !== "Không có")
            .map((row) => {
              const order = orders.find((o) => o.item === row.Item_Deleted);
              return order.id;
            });

          if (allIdsToDelete.length) {
            await Order.update(
              { deleted_at: new Date() },
              { where: { id: allIdsToDelete }, transaction }
            );
          }
        }

        await transaction.commit();
      }
    } catch (error) {
      await transaction.rollback();
      logger.error(`Lỗi cập nhật database: ${conversationId}: ${error.message}`);
      throw error;
    }
    return;
  }

  const ordersString = formatOrdersStringForBatch({ [conversationId]: groupsToProcess });
  if (!ordersString) {
    logger.info(`Không có dữ liệu mới: ${conversationId}`);
    return;
  }

  try {
    const content = await callOpenAIApi([conversationId], ordersString);
    const result = parseJsonSafely(content, [conversationId], { [conversationId]: groupsToProcess }, { [conversationId]: groupCacheHits });

    const transaction = await sequelize.transaction();
    try {
      for (const convResult of result) {
        const tableData = [];
        let totalDeleted = 0;

        for (const groupResult of convResult.groups) {
          const keptOrderIds = groupResult.uniqueItems.map((order) => order.ID);
          const groupIndex = groupResult.group - 1;
          const originalGroup = groupsWithDuplicates[groupIndex];

          const idsToDelete = originalGroup
            .map((order) => order.id)
            .filter((id) => !keptOrderIds.includes(id));

          totalDeleted += idsToDelete.length;

          for (const id of idsToDelete) {
            const order = originalGroup.find((o) => o.id === id);
            tableData.push({
              Conversation_ID: conversationId,
              Group: groupResult.group,
              Item_Deleted: order.item,
              Item_Kept: "Không có",
            });
          }

          for (const keptOrder of groupResult.uniqueItems) {
            tableData.push({
              Conversation_ID: conversationId,
              Group: groupResult.group,
              Item_Deleted: "Không có",
              Item_Kept: keptOrder.Item,
            });
          }
        }

        if (tableData.length) {
          logger.info(`\nKết quả cho Conversation ID: ${conversationId}`);
          logger.info("-".repeat(120));
          console.table(tableData, ["Conversation_ID", "Group", "Item_Deleted", "Item_Kept"]);
          logger.info(`Đã xóa ${totalDeleted} bản ghi trùng lặp cho Conversation ID: ${conversationId}`);

          const allIdsToDelete = tableData
            .filter((row) => row.Item_Deleted !== "Không có")
            .map((row) => {
              const order = orders.find((o) => o.item === row.Item_Deleted);
              return order.id;
            });

          if (allIdsToDelete.length) {
            await Order.update(
              { deleted_at: new Date() },
              { where: { id: allIdsToDelete }, transaction }
            );
          }
        }

        await transaction.commit();
      }
    } catch (error) {
      await transaction.rollback();
      logger.error(`Lỗi cập nhật database: ${conversationId}: ${error.message}`);
      throw error;
    }
  } catch (error) {
    logger.error(`Lỗi xử lý Conversation ID ${conversationId}: ${error.message}`);
  } finally {
    const runTime = (Date.now() - startTime) / 1000;
    logger.info(`Kết thúc Conversation ID: ${conversationId}. Thời gian: ${runTime} giây`);
  }
}

// Hàm chính
async function main() {
  const startTime = Date.now();
  logger.info(`Chương trình bắt đầu`);

  try {
    await ensureJsonLogDir(); // Tạo thư mục logs/json trước khi chạy
    await loadCache();
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const orders = await fetchOrdersBatch(offset, CONFIG.BATCH_LIMIT);
      if (!orders.length) {
        hasMore = false;
        break;
      }

      const ordersGrouped = orders.reduce((acc, order) => {
        acc[order.conversation_id] = acc[order.conversation_id] || [];
        acc[order.conversation_id].push(order);
        return acc;
      }, {});

      let currentBatch = {};
      let currentTokenCount = 0;
      let batches = [];

      for (const [conversationId, orders] of Object.entries(ordersGrouped)) {
        const groups = processLocalDuplicates(groupSimilarItems(orders)).filter((g) => g.length > 1);
        if (!groups.length) continue;

        const batchString = formatOrdersStringForBatch({ [conversationId]: groups });
        const tokenCount = estimateTokens(batchString);

        if (
          Object.keys(currentBatch).length >= CONFIG.MAX_CONVERSATION_IDS_PER_BATCH ||
          currentTokenCount + tokenCount > CONFIG.MAX_TOKEN_PER_BATCH
        ) {
          batches.push(currentBatch);
          currentBatch = {};
          currentTokenCount = 0;
        }

        currentBatch[conversationId] = groups;
        currentTokenCount += tokenCount;
      }

      if (Object.keys(currentBatch).length) {
        batches.push(currentBatch);
      }

      batches.sort((a, b) => Object.keys(b).length - Object.keys(a).length);

      const batchPromises = batches.map((batch) => ({
        status: "pending",
        value: processConversationBatch(batch),
      }));

      const results = await Promise.allSettled(batchPromises);
      for (const result of results) {
        if (result.status === "rejected") {
          logger.error(`Lỗi batch: ${result.reason}`);
        }
      }

      offset += CONFIG.BATCH_LIMIT;
    }
  } catch (error) {
    logger.error(`Lỗi chính: ${error.message}`);
  } finally {
    await saveCache();
    const runTime = (Date.now() - startTime) / 1000;
    const cacheHitRate = cacheHits / (cacheHits + cacheMisses) || 0;
    logger.info(`Chương trình kết thúc. Tổng thời gian: ${runTime} giây, Tổng token đã tiêu hao: ${totalTokensUsed}`);
    logger.info(`Cache hit: ${cacheHits}, Cache miss: ${cacheMisses}, Tỷ lệ cache hit: ${(cacheHitRate * 100).toFixed(2)}%`);
  }
}

main().catch((error) => logger.error(`Lỗi nghiêm trọng: ${error.message}`));