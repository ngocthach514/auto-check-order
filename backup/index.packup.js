require("dotenv").config();
const axios = require("axios");
const { Sequelize, DataTypes, Op } = require("sequelize");
const { default: PQueue } = require("p-queue");

const sequelize = new Sequelize("nguyenkim-autozalo", "root", "", {
  host: "localhost",
  dialect: "mysql",
  port: 3306,
  logging: false,
});

const Order = sequelize.define(
  "orders",
  {
    id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
    conversation_id: DataTypes.INTEGER,
    item: DataTypes.STRING,
    status: DataTypes.STRING,
    deleted_at: DataTypes.DATE,
    created_at: DataTypes.DATE,
  },
  { timestamps: false }
);

const apiKey = process.env.OPENAI_API_KEY;
const headers = {
  Authorization: `Bearer ${apiKey}`,
  "Content-Type": "application/json",
};

const queues = Array.from(
  { length: 10 },
  (_, i) => new PQueue({ concurrency: 5, interval: 500, intervalCap: 10 })
);

async function fetchOrdersBatch(offset = 0, limit = 2000) {
  console.info(`Đang lấy dữ liệu từ offset ${offset} với limit ${limit}...`);
  try {
    const threeDaysAgo = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000);

    const conversationIds = await Order.findAll({
      attributes: ["conversation_id"],
      where: {
        status: "pending",
        deleted_at: null,
        created_at: { [Op.gte]: threeDaysAgo },
      },
      group: ["conversation_id"],
      having: sequelize.literal("COUNT(*) >= 2"),
      raw: true,
    });

    const conversationIdList = conversationIds.map(
      (item) => item.conversation_id
    );

    const orders = await Order.findAll({
      where: {
        conversation_id: { [Op.in]: conversationIdList },
        status: "pending",
        deleted_at: null,
        created_at: { [Op.gte]: threeDaysAgo },
      },
      order: [["conversation_id", "ASC"]],
      limit,
      offset,
      raw: true,
    });

    console.info(`Đã lấy được ${orders.length} bản ghi.`);
    return orders;
  } catch (error) {
    console.error(`Lỗi khi lấy dữ liệu: ${error.message}`);
    return [];
  }
}

function formatOrdersString(orders) {
  return orders
    .filter((order) => order.id && order.conversation_id && order.item)
    .map(
      (order) =>
        `ID: ${order.id}, Conversation ID: ${order.conversation_id}, Item: ${order.item}`
    )
    .join("\n");
}

async function callOpenAIApi(conversationId, ordersString, retries = 3) {
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

**VÍ DỤ CỤ THỂ**
- Đối với các sản phẩm Dell OptiPlex:
  - "7020MT" là Mini Tower, trong khi "7020 SFF" là Small Form Factor, đây là hai form factor khác nhau.
  - CPU như "Core i3-12100" và "Core i3-14100" là các thế hệ khác nhau với hiệu suất khác nhau.
  - Hệ điều hành như "Ubuntu" và "Windows 11 Home" là khác nhau.

**KẾT LUẬN MONG MUỐN**
Dựa trên sự tương đồng hoặc khác biệt của các từ khóa và đặc điểm, đưa ra nhận định liệu các "Item" này có cùng chỉ một sản phẩm hay không.

**LƯU Ý QUAN TRỌNG**
- Hãy xử lí rồi trả kết quả là chuỗi JSON như dữ liệu đầu ra mong muốn phía dưới, không cần giải thích phân tích hoặc kết luận
- Trả về danh sách các sản phẩm không trùng lặp, giữ lại sản phẩm có mô tả đầy đủ nhất cho mỗi nhóm trùng lặp:

**CẤU TRÚC DỮ LIỆU ĐẦU VÀO**
"ID: 22305, Conversation ID: 4085, Item: Win Pro 11 64Bit Eng Intl 1pk DSP OEI DVD (FQC-10528)"
"ID: 22306, Conversation ID: 4085, Item: DG7GMGF0L4TL Windows fos - Windows 11 Pro - Legalization Get Genuine"
"ID: 22307, Conversation ID: 4085, Item: Windows 11 Pro - Legalization Get Genuine"

**DỮ LIỆU ĐẦU RA MONG MUỐN**
[
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
]`;

  for (let i = 0; i < retries; i++) {
    try {
      await new Promise((resolve) =>
        setTimeout(resolve, 1000 * Math.pow(2, i))
      );
      const response = await axios.post(
        "https://api.openai.com/v1/chat/completions",
        {
          model: "gpt-4o-mini-2024-07-18",
          messages: [
            { role: "system", content: systemMessage },
            { role: "user", content: ordersString },
          ],
        },
        { headers, timeout: 60000 }
      );
      const content = response.data.choices[0].message.content || "{}";
      return content;
    } catch (error) {
      console.error(
        `Lỗi khi gọi API OpenAI cho Conversation ID ${conversationId}: ${error.message}`
      );
      // if (error.response?.status === 429) {
      //   console.warn(`Rate limit reached for Conversation ID ${conversationId}. Retrying after ${Math.pow(2, i)} seconds...`);
      // } else if (i === retries - 1) {
      //   console.error(`Lỗi khi gọi API OpenAI cho Conversation ID ${conversationId}: ${error.message}`);
      //   throw error;
      // }
    }
  }
  throw new Error(`Max retries reached for Conversation ID ${conversationId}`);
}

function parseJsonSafely(content, conversationId, orders) {
  try {
    const parsed = JSON.parse(content);
    if (!Array.isArray(parsed)) {
      console.warn(
        `JSON không phải mảng cho Conversation ID ${conversationId}: ${content}`
      );
      return orders;
    }

    const seenItems = new Map();
    const uniqueOrders = [];
    for (const order of parsed) {
      const item = order.Item.toLowerCase();
      if (seenItems.has(item)) {
        const existing = seenItems.get(item);
        if (order.ID < existing.ID) {
          seenItems.set(item, order);
        }
      } else {
        seenItems.set(item, order);
        uniqueOrders.push(order);
      }
    }

    return uniqueOrders;
  } catch (error) {
    console.error(
      `Không có sản phẩm trùng cho Conversation ID ${conversationId}: ${error.message}. Giữ nguyên danh sách gốc.`
    );
    return orders;
  }
}

async function processConversationGroup(conversationId, orders) {
  const startTime = new Date();
  console.info(
    `Bắt đầu xử lý Conversation ID: ${conversationId} lúc ${startTime.toLocaleString()}`
  );

  const ordersString = formatOrdersString(orders);
  if (!ordersString) {
    console.info(
      `Không có sản phẩm nào cho Conversation ID: ${conversationId}`
    );
    return;
  }

  try {
    const content = await callOpenAIApi(conversationId, ordersString);
    const duplicates = parseJsonSafely(content, conversationId, orders);

    const keptOrderIds = duplicates.map((order) => order.ID);
    const idsToDelete = orders
      .map((order) => order.id)
      .filter((id) => !keptOrderIds.includes(id));

    if (idsToDelete.length || duplicates.length) {
      console.info(`\nKết quả cho Conversation ID: ${conversationId}`);
      console.info("-".repeat(120));

      const tableData = [];

      for (const id of idsToDelete) {
        const order = orders.find((o) => o.id === id);
        tableData.push({
          Conversation_ID: conversationId,
          Item_Deleted: order.item,
          Item_Kept: "Không có",
        });
      }

      for (const keptOrder of duplicates) {
        tableData.push({
          Conversation_ID: conversationId,
          Item_Deleted: "Không có",
          Item_Kept: keptOrder.Item,
        });
      }

      console.table(tableData, [
        "Conversation_ID",
        "Item_Deleted",
        "Item_Kept",
      ]);
      console.info(
        `Đã xóa ${idsToDelete.length} bản ghi trùng lặp cho Conversation ID: ${conversationId}`
      );
    } else {
      console.info(
        `Không có bản ghi trùng lặp cho Conversation ID: ${conversationId}`
      );
    }

    if (idsToDelete.length) {
      await Order.update(
        { deleted_at: new Date() },
        { where: { id: idsToDelete } },
        { reason: "Hệ thống tự động xóa bản ghi trùng lặp" }
      );
    }
  } catch (error) {
    console.error(
      `Lỗi khi xử lý Conversation ID ${conversationId}: ${error.message}`
    );
  } finally {
    const endTime = new Date();
    const runTime = (endTime - startTime) / 1000;
    console.info(
      `Kết thúc xử lý Conversation ID: ${conversationId} lúc ${endTime.toLocaleString()}. Thời gian chạy: ${runTime} giây`
    );
  }
}

async function main() {
  const startTime = new Date();
  console.info(`Chương trình bắt đầu lúc: ${startTime.toLocaleString()}`);

  try {
    let offset = 0;
    const limit = 1000;
    let hasMore = true;

    while (hasMore) {
      const orders = await fetchOrdersBatch(offset, limit);
      if (!orders.length) {
        hasMore = false;
        break;
      }

      const ordersGrouped = orders.reduce((acc, order) => {
        acc[order.conversation_id] = acc[order.conversation_id] || [];
        acc[order.conversation_id].push(order);
        return acc;
      }, {});

      const tasks = Object.entries(ordersGrouped);
      let queueIndex = 0;

      for (const [conversationId, orders] of tasks) {
        const queue = queues[queueIndex % 5];
        queue.add(() => processConversationGroup(conversationId, orders));
        queueIndex++;
      }

      offset += limit;
    }

    await Promise.all(queues.map((q) => q.onIdle()));
  } catch (error) {
    console.error(`Lỗi trong quá trình chính: ${error.message}`);
  } finally {
    const endTime = new Date();
    const runTime = (endTime - startTime) / 1000;
    console.info(`Chương trình kết thúc lúc: ${endTime.toLocaleString()}`);
    console.info(`Tổng thời gian chạy: ${runTime} giây`);
  }
}

main().catch((error) => console.error(`Lỗi nghiêm trọng: ${error.message}`));
