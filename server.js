import express from "express";

const app = express();
app.use(express.json());

app.post("/api/webhooks/whatsapp", (req, res) => {
  console.log("🔥 WEBHOOK RECEBIDO:");
  console.log(req.body);

  res.sendStatus(200);
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`🚀 Server rodando na porta ${PORT}`);
});
