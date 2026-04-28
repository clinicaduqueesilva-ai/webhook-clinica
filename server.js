import express from "express";

// Set Brazil Timezone globally for this process
process.env.TZ = 'America/Sao_Paulo';

// Global Error Handling - Install at the very beginning
process.on('unhandledRejection', (reason, promise) => {
  console.error('[CRITICAL] Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('uncaughtException', (err) => {
  console.error('[CRITICAL] Uncaught Exception:', err);
});

import { createServer as createViteServer } from "vite";
import path from "path";
import { fileURLToPath } from "url";
import { WhatsAppQueueItem } from "./src/types.ts";
import { whatsappProvider } from "./src/services/whatsappProvider.ts";
import { dbService } from "./src/services/dbService.ts";
import { formatDateForDisplay } from "./src/services/whatsappService.ts";

console.log(`[WhatsApp Backend] Unified dbService initialized.`);

// NOTE: We no longer force reset quota on boot. 
// If the server restarts due to a build change, we want to stay cautious.
// Delaying init slightly to avoid startup storm
setTimeout(() => {
  whatsappProvider.init();
}, 5000);

async function processWhatsAppQueue() {
  if (dbService.isQuotaExhausted()) return;
  
  // SEÇÃO DE AUDITORIA TÉCNICA:
  // Se o WhatsApp não estiver conectado, não faz sentido processar a fila.
  // Isso economiza centenas de gravações por hora se o número estiver offline.
  if (whatsappProvider.getConnectionStatus() !== 'connected') {
    // console.log('[WhatsApp Queue] Suspendendo processamento: WhatsApp não está conectado.');
    return;
  }
  
  try {
    const colName = "whatsapp_queue";
    
    const messages = await dbService.getWhere(colName, "status", "==", "pending");
    
    if (messages.length === 0) {
      return;
    }

    console.log(`[WhatsApp Queue][PROCESS] Found ${messages.length} pending messages.`);
    
    for (const item of messages) {
      if (item.attempts >= 3) {
        await dbService.update(colName, item.id, { status: "failed", error: "Max attempts reached" });
        continue;
      }

      let result;
      try {
        result = await whatsappProvider.sendMessage(item.to, item.message);
      } catch (err) {
        result = { success: false, error: err instanceof Error ? err.message : 'Unknown error' };
      }

      const updateData = result.success ? {
        status: "sent",
        lastAttempt: new Date().toISOString()
      } : {
        attempts: (item.attempts || 0) + 1,
        lastAttempt: new Date().toISOString(),
        error: result.error
      };

      await dbService.update(colName, item.id, updateData);
      
      if (result.success) {
        // OTIMIZAÇÃO: Não gravar logs técnicos no Firestore.
        // O status "sent" na própria fila já é suficiente para rastreio.
        console.log(`[WhatsApp Queue][AUDIT] Sucesso: Mensagem entregue para ${item.to}. Registro em memória apenas.`);
      }
    }
  } catch (error: any) {
    console.error("[WhatsApp Queue] Critical Error:", error.message);
  }
}

// Helper to validate and format phone numbers for Uazapi
function validateAndFormatPhone(phone: string | undefined): string | null {
  if (!phone) return null;
  // Remove all non-digits
  let digits = phone.replace(/\D/g, '');
  if (digits.length < 10) return null; // Too short
  
  // Ensure Brazil country code if missing
  if (digits.length === 11 && !digits.startsWith('55')) {
    digits = '55' + digits;
  }
  
  return digits;
}

// Helper to format YYYY-MM-DD to DD-MM-YYYY
function formatToBRDate(dateString: string): string {
  if (!dateString) return '';
  const parts = dateString.split('-');
  if (parts.length !== 3) return dateString;
  const [year, month, day] = parts;
  return `${day}-${month}-${year}`;
}

// Background worker for instant notifications (Creation/Update)
async function automateInstantNotifications() {
  if (dbService.isQuotaExhausted()) return;
  
  // SEÇÃO DE AUDITORIA TÉCNICA:
  // Se o WhatsApp não estiver conectado, não faz sentido buscar agendamentos.
  if (whatsappProvider.getConnectionStatus() !== 'connected') return;

  try {
    // 1. Get recent scheduled appointments
    const allApps = await dbService.getAll("agendamentos");
    const appointments = allApps.filter(app => app.status === "confirmed" || app.status === "Agendado");
    
    for (const app of appointments as any[]) {
      const needsPatient = !app.notificadoCriacaoPac;
      const needsProf = !app.notificadoCriacaoProf;

      if (!needsPatient && !needsProf) continue;

      // Duplicate Checks with Logging
      if (!needsProf) {
        console.log(`[WHATSAPP][PROFESSIONAL][SKIP_DUPLICATE] Agendamento ${app.id} já possui notificadoCriacaoProf=true.`);
      }
      
      if (!needsPatient) {
        console.log(`[WHATSAPP][PATIENT][SKIP_DUPLICATE] Agendamento ${app.id} já possui notificadoCriacaoPac=true.`);
      }

      console.log(`[UAZAPI][STATUS] Inicando pack de notificações para: ${app.id}`);

      // Fetch dependencies
      const patient = await dbService.getDocument("patients", app.patientId);
      const professional = await dbService.getDocument("professionals", app.professionalId);

      if (!patient || !professional) {
        console.warn(`[UAZAPI][ERROR] Dados incompletos para app ${app.id}. Paciente: ${!!patient}, Prof: ${!!professional}`);
        continue;
      }

      const { formatWhatsAppMessage, formatPatientConfirmationMessage } = await import('./src/services/whatsappService.ts');
      const baseUrl = process.env.APP_URL || 'http://localhost:3000';

      const updatePayload: any = {};

      // Patient Notification
      if (needsPatient) {
        const phone = validateAndFormatPhone(patient.phone);
        if (phone) {
          const msg = formatPatientConfirmationMessage(patient, app, baseUrl);
          await dbService.save("whatsapp_queue", {
            to: phone,
            message: msg,
            appointmentId: app.id,
            type: "creation",
            status: "pending",
            attempts: 0,
            createdAt: new Date().toISOString()
          });
          updatePayload.notificadoCriacaoPac = true;
          updatePayload.notificadoCriacaoPacEm = new Date().toISOString();
          updatePayload.mensagem_enviada = true;
          console.log(`[UAZAPI][SEND][PATIENT] Enviado para fila: ${patient.name}`);
        } else {
          updatePayload.notificadoCriacaoPac = 'invalid_phone';
        }
      }

      // Professional Notification
      if (needsProf) {
        const phone = validateAndFormatPhone(professional.phone);
        if (phone) {
          console.log(`[WHATSAPP][PROFESSIONAL][SEND_START] Preparando envio para Dr. ${professional.name} (App: ${app.id})`);
          const msg = formatWhatsAppMessage('creation', professional, patient, app);
          await dbService.save("whatsapp_queue", {
            to: phone,
            message: msg,
            appointmentId: app.id,
            type: "creation",
            status: "pending",
            attempts: 0,
            createdAt: new Date().toISOString()
          });
          updatePayload.notificadoCriacaoProf = true;
          updatePayload.notificadoCriacaoProfEm = new Date().toISOString();
          console.log(`[WHATSAPP][PROFESSIONAL][SEND_SUCCESS] Enviado para fila: Dr. ${professional.name}`);
        } else {
          updatePayload.notificadoCriacaoProf = 'invalid_phone';
          console.warn(`[WHATSAPP][PROFESSIONAL][SEND_ERROR] Profissional sem telefone válido: ${professional.name}`);
        }
      }

      if (Object.keys(updatePayload).length > 0) {
        await dbService.update("agendamentos", app.id, updatePayload);
      }
    }
  } catch (error: any) {
    console.error("[UAZAPI][ERROR] Instant automation failed:", error.message);
  }
}

// Background worker for confirmations (24h and 3h reminders)
async function automateReminders(isManual = false) {
  if (dbService.isQuotaExhausted()) return { success: false, reason: "quota_exhausted" };
  if (whatsappProvider.getConnectionStatus() !== 'connected') {
    return { success: false, reason: "whatsapp_disconnected" };
  }

  const skipped: any[] = [];
  const sent: any[] = [];
  let sent24h = 0;
  let sent3h = 0;

  try {
    const now = new Date();
    const nowTs = now.getTime();
    
    console.log("[DEBUG][NOW_LOCAL]", now.toLocaleString("pt-BR"));
    console.log(`[REMINDER_WORKER][RUNNING] now=${now.toISOString()}${isManual ? ' [MANUAL]' : ''}`);
    
    const allApps = await dbService.getAll("agendamentos");
    const excludedStatuses = ['Cancelado', 'Remarcado', 'Faltou', 'Finalizado', 'Compareceu', 'cancelled', 'rescheduled', 'no-show', 'showed'];

    for (const app of allApps as any[]) {
      if (!app.patientId || !app.startTime || !app.date) continue;
      
      const appStatus = (app.status || '').toString();
      const isExcluded = excludedStatuses.some(s => appStatus.toLowerCase() === s.toLowerCase());
      
      const [yearStr, monthStr, dayStr] = app.date.split('-');
      const [hourStr, minuteStr] = app.startTime.split(':');
      
      const appDate = new Date(
        Number(yearStr),
        Number(monthStr) - 1,
        Number(dayStr),
        Number(hourStr),
        Number(minuteStr),
        0,
        0
      );
      const appTs = appDate.getTime();
      
      const diffMs = appTs - nowTs;
      const diffMinutes = Math.floor(diffMs / 60000);

      console.log("[DEBUG][COMPARE]", {
        id: app.id,
        appointment: appDate.toISOString(),
        now: now.toISOString(),
        diffMinutes
      });

      // DEBUG LOGS REQUESTED BY USER
      if (isManual || (diffMinutes > -60 && diffMinutes < 1500)) {
        console.log("[DEBUG][DATE_VALUES]", {
          id: app.id,
          patientName: app.patientName,
          rawDate: app.date,
          rawStartTime: app.startTime,
          constructedDateISO: appDate.toISOString(),
          nowISO: now.toISOString(),
          diffMinutes
        });
      }

      // Fetch patient phone early for logs
      const patientData = await dbService.getDocument("patients", app.patientId);
      const phone = patientData?.phone || '';
      const formattedPhone = validateAndFormatPhone(phone);
      
      const logInfo = {
        appointmentId: app.id,
        patientName: app.patientName,
        date: app.date,
        startTime: app.startTime,
        status: appStatus,
        phone: phone,
        diffMinutes,
        lembrete24hEnviado: !!app.lembrete24hEnviado,
        lembrete3hEnviado: !!app.lembrete3hEnviado
      };

      if (isExcluded) {
        skipped.push({ ...logInfo, reason: "status_nao_elegivel" });
        continue;
      }
      
      if (diffMinutes < 0) {
        skipped.push({ ...logInfo, reason: "consulta_passada" });
        continue;
      }

      // Check for 24h Reminder
      if (diffMinutes >= 1410 && diffMinutes <= 1470) {
        if (app.lembrete24hEnviado) {
          skipped.push({ ...logInfo, reason: "ja_enviado_24h" });
          continue;
        }

        if (!formattedPhone) {
          skipped.push({ ...logInfo, reason: "telefone_invalido" });
          continue;
        }

        try {
          const api = (whatsappProvider as any).ensureUazapi();
          const brDate = formatToBRDate(app.date);
          const msg = `Olá, ${app.patientName}! 😊

Estamos passando para lembrar da sua consulta amanhã na Clínica Duque e Silva.

📅 Data: ${brDate}
⏰ Horário: ${app.startTime}
👩‍⚕️ Profissional: ${app.professional || 'Profissional'}

Por favor, escolha uma opção abaixo:`;
          
          await api.sendMenu(formattedPhone, msg, ["Confirmar", "Desmarcar", "Reagendar"]);
          
          await dbService.update("agendamentos", app.id, {
            lembrete24hEnviado: true,
            lembrete24hEnviadoEm: new Date().toISOString()
          });
          
          sent24h++;
          sent.push({ ...logInfo, type: '24h', phone: formattedPhone });
        } catch (err: any) {
          skipped.push({ ...logInfo, reason: "erro_envio", error: err.message });
        }
        continue;
      }

      // Check for 3h Reminder
      if (diffMinutes >= 170 && diffMinutes <= 190) {
        if (app.lembrete3hEnviado) {
          skipped.push({ ...logInfo, reason: "ja_enviado_3h" });
          continue;
        }

        if (!formattedPhone) {
          skipped.push({ ...logInfo, reason: "telefone_invalido" });
          continue;
        }

        try {
          const api = (whatsappProvider as any).ensureUazapi();
          const msg = `Olá, ${app.patientName}! 😊

Sua consulta na Clínica Duque e Silva será em breve.

⏰ Horário: ${app.startTime}
👩‍⚕️ Profissional: ${app.professional || 'Profissional'}

Por favor, escolha uma opção abaixo:`;
          
          await api.sendMenu(formattedPhone, msg, ["Confirmar", "Desmarcar", "Reagendar"]);
          
          await dbService.update("agendamentos", app.id, {
            lembrete3hEnviado: true,
            lembrete3hEnviadoEm: new Date().toISOString()
          });
          
          sent3h++;
          sent.push({ ...logInfo, type: '3h', phone: formattedPhone });
        } catch (err: any) {
          skipped.push({ ...logInfo, reason: "erro_envio", error: err.message });
        }
        continue;
      }
      
      skipped.push({ ...logInfo, reason: "fora_da_janela" });
    }

    return { 
      success: true,
      actionType: "RUN_REMINDERS_JOB",
      checkedAppointments: allApps.length,
      sent24h,
      sent3h,
      skipped,
      sent
    };
  } catch (error: any) {
    console.error("[WHATSAPP][REMINDERS][ERROR]", error.message);
    return { success: false, error: error.message };
  }
}

// Helper to normalize phone numbers for matching
function normalizePhone(phone: string): string {
  // Remove all non-digits
  let digits = phone.replace(/\D/g, '');
  // Many Brazilian numbers come with '55' prefix from WhatsApp
  if (digits.startsWith('55') && digits.length > 11) {
    digits = digits.substring(2);
  }
  return digits;
}

// WhatsApp Message Listener
whatsappProvider.on('message', async (upsert: any) => {
  try {
    const rawData = upsert.messages[0];
    if (!rawData.message || rawData.key.fromMe) return;

    // 1. LOG RAW MESSAGE FOR INVESTIGATION (Requested)
    console.log("[WEBHOOK][RAW]", JSON.stringify(rawData, null, 2));

    const from = rawData.key.remoteJid;
    const rawPhone = from.split('@')[0];
    const normalizedFrom = normalizePhone(rawPhone);
    console.log("[WEBHOOK][FROM_PHONE]", rawPhone);
    
    // 2. EXTRACT RESPONSE FROM MULTIPLE POSSIBLE FIELDS (Uazapi V2 / Evolution API)
    const m = rawData.message;
    
    // Text fields
    const text = (m.conversation || m.extendedTextMessage?.text || m.text || m.content || "").trim();
    
    // Interactive button/menu responses
    const interactive = m.interactiveResponse || {};
    const selectedButtonId = m.buttonsResponseMessage?.selectedButtonId || 
                             m.templateButtonReplyMessage?.selectedId ||
                             m.listResponseMessage?.singleSelectReply?.selectedRowId ||
                             interactive.body?.text ||
                             interactive.nativeFlowResponseMessage?.paramsJson ||
                             m.buttonOrListid || 
                             "";
                             
    const selectedButtonText = m.buttonsResponseMessage?.selectedDisplayText || 
                               m.listResponseMessage?.title ||
                               interactive.body?.text ||
                               m.buttonText || 
                               "";
    
    // Normalize to one response string
    const detectedResponse = (selectedButtonId || selectedButtonText || text).trim();
    if (!detectedResponse) return;

    console.log("[WEBHOOK][NORMALIZED_TEXT]", detectedResponse);

    // MAP RESPONSE TO ACTION
    let actionType: "Confirmado" | "Cancelado" | "Remarcado" | null = null;
    const norm = detectedResponse.toLowerCase();

    if (norm.includes('confirm')) {
      console.log("[WEBHOOK][FLOW] CONFIRM DETECTED");
      actionType = "Confirmado";
    } else if (norm.includes('desmarcar') || norm.includes('cancel') || text === '2') {
      actionType = "Cancelado";
    } else if (norm.includes('reagendar') || norm.includes('remarcar') || norm.includes('reschedule')) {
      actionType = "Remarcado";
    }

    console.log("[WEBHOOK][ACTION_DETECTED]", actionType);
    if (!actionType) return;

    // 4. FIND THE CORRECT APPOINTMENT
    // Search patients by phone to find their IDs
    const allPatients = await dbService.getAll("patients");
    const matchingPatients = allPatients.filter(p => p.phone && normalizePhone(p.phone) === normalizedFrom);
    const matchingPatientIds = matchingPatients.map(p => p.id);

    // Get all appointments
    const allApps = await dbService.getAll("agendamentos");
    const nowLocal = new Date();
    
    // Filter for future, relevant appointments for this patient
    const candidateApps = (allApps as any[])
      .filter(a => {
        // Match by patient ID or direct phone if available
        const idMatch = a.patientId && matchingPatientIds.includes(a.patientId);
        const phoneMatch = a.patientPhone && normalizePhone(a.patientPhone) === normalizedFrom;
        
        if (!idMatch && !phoneMatch) return false;
        
        // Skip past appointments (past 2 hours)
        const [y, m, d] = a.date.split('-').map(Number);
        const [h, min_val] = (a.startTime || "00:00").split(':').map(Number);
        const appDate = new Date(y, m-1, d, h, min_val);
        if (appDate.getTime() < nowLocal.getTime() - (2 * 60 * 60 * 1000)) return false; 
        
        return true;
      })
      .sort((a, b) => {
        // Prefer those that had reminders sent
        const aHasRem = (a.lembrete24hEnviado || a.lembrete3hEnviado) ? 1 : 0;
        const bHasRem = (b.lembrete24hEnviado || b.lembrete3hEnviado) ? 1 : 0;
        if (aHasRem !== bHasRem) return bHasRem - aHasRem;
        
        // Sort by date/time ascending (nearest first)
        const ad = new Date(a.date.split('-').map(Number)[0], a.date.split('-').map(Number)[1]-1, a.date.split('-').map(Number)[2], a.startTime.split(':').map(Number)[0], a.startTime.split(':').map(Number)[1]);
        const bd = new Date(b.date.split('-').map(Number)[0], b.date.split('-').map(Number)[1]-1, b.date.split('-').map(Number)[2], b.startTime.split(':').map(Number)[0], b.startTime.split(':').map(Number)[1]);
        return ad.getTime() - bd.getTime();
      });

    if (candidateApps.length === 0) {
      console.log(`[WEBHOOK][PATIENT_RESPONSE][NO_APPOINTMENT_FOUND] Phone: ${normalizedFrom}`);
      return;
    }

    const targetApp = candidateApps[0];
    const patientName = targetApp.patientName || (matchingPatients.find(p => p.id === targetApp.patientId)?.name) || "Paciente";
    
    console.log("[WEBHOOK][APPOINTMENT_FOUND]", targetApp.id, targetApp);

    // 5. CHECK FOR DUPLICATES
    if (targetApp.respostaPacienteRecebida === true) {
      console.log(`[WEBHOOK][PATIENT_RESPONSE][SKIP_DUPLICATE] App: ${targetApp.id}`);
      return;
    }

    // 6. UPDATE STATUS
    const logTag = actionType === "Confirmado" ? "CONFIRMED" : actionType === "Cancelado" ? "CANCELED" : "RESCHEDULED";
    console.log(`[WEBHOOK][PATIENT_RESPONSE][${logTag}] App: ${targetApp.id} | Patient: ${patientName} | Response: ${detectedResponse}`);
    
    await dbService.update("agendamentos", targetApp.id, {
      status: actionType,
      respostaPacienteRecebida: true,
      respostaPacienteRecebidaEm: new Date().toISOString(),
      respostaPacienteTipo: actionType.toLowerCase()
    });

    console.log(`[WEBHOOK][DB_UPDATED] ${actionType}`);

    // Verify final state
    const appointmentAfterUpdate = await dbService.getDocument("agendamentos", targetApp.id);
    console.log("[WEBHOOK][AFTER_UPDATE]", appointmentAfterUpdate);

    // 7. SEND AUTOMATIC REPLY
    let replyMsg = "";
    if (actionType === "Confirmado") {
      replyMsg = `Perfeito, ${patientName}! Sua presença está confirmada. ✅`;
    } else if (actionType === "Cancelado") {
      replyMsg = `Tudo bem, ${patientName}. Seu agendamento foi cancelado. Caso queira marcar novamente, estamos à disposição.`;
    } else if (actionType === "Remarcado") {
      replyMsg = `Sem problemas, ${patientName}. Nossa equipe vai te chamar para encontrar um novo horário. 😊`;
    }

    if (replyMsg) {
      // Send directly via provider as it's a response to a real incoming message
      await whatsappProvider.sendMessage(from, replyMsg);
    }

  } catch (error: any) {
    console.error(`[WEBHOOK][ERROR] WhatsApp Listener: ${error.message}`);
  }
});

async function startServer() {
  const app = express();
  const PORT = 3000;

  app.use(express.json());

  // API Routes
  app.get("/api/health", (req, res) => {
    res.json({ 
      status: "ok", 
      timestamp: new Date().toISOString(),
      quotaExhausted: dbService.isQuotaExhausted()
    });
  });

  // WhatsApp Notification Trigger
  app.post("/api/whatsapp/notify", async (req, res) => {
    if (dbService.isQuotaExhausted()) {
      return res.status(503).json({ error: "Serviço temporariamente indisponível: Cota de banco esgotada." });
    }

    const { to, message, appointmentId, type, recipientType } = req.body;
    
    if (!to || !message || !appointmentId || !type) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    // Check if professional notification already sent to avoid duplicates (Instant trigger)
    if (recipientType === 'professional') {
      const app = await dbService.getDocument("agendamentos", appointmentId);
      if (app && app.notificadoCriacaoProf === true) {
        console.log(`[WHATSAPP][PROFESSIONAL][SKIP_DUPLICATE] Agendamento ${appointmentId} já notificado (API Trigger).`);
        return res.json({ status: "already_sent" });
      }
      console.log(`[WHATSAPP][PROFESSIONAL][SEND_START] API Trigger: Iniciando envio para ${to}`);
    }

    const formattedPhone = validateAndFormatPhone(to);
    if (!formattedPhone) {
      console.warn(`[UAZAPI][NOTIFY] Telefone inválido: ${to}. Operação abortada para app ${appointmentId}.`);
      return res.status(400).json({ error: "Invalid phone number" });
    }

    try {
      // Try to send immediately if connected
      const status = whatsappProvider.getStatus();
      if (status.status === 'connected') {
        try {
          await whatsappProvider.sendMessage(formattedPhone, message);
          
          // Log SUCCESS
          console.log(`[UAZAPI][LOG] Sucesso: Mensagem enviada para ${formattedPhone} (App: ${appointmentId})`);
          
          await dbService.save("whatsapp_logs", {
            to: formattedPhone,
            message,
            appointmentId,
            status: "sent",
            timestamp: new Date().toISOString()
          });

          // Mark appointment as notified to prevent duplicates
          if (type === 'creation') {
            const updateData: any = {
              notificadoCriacaoPac: true,
              notificadoCriacaoPacEm: new Date().toISOString(),
              mensagem_enviada: true // Requested flag
            };
            
            if (recipientType === 'professional') {
              updateData.notificadoCriacaoProf = true;
              updateData.notificadoCriacaoProfEm = new Date().toISOString();
              console.log(`[WHATSAPP][PROFESSIONAL][SEND_SUCCESS] Notificação confirmada para Profissional (App: ${appointmentId})`);
            }

            await dbService.update("agendamentos", appointmentId, updateData);
          }

          return res.json({ status: "sent" });
        } catch (err: any) {
          console.error(`[UAZAPI][ERROR] Envio imediato falhou: ${err.message}. Enfileirando...`);
          
          await dbService.save("whatsapp_logs", {
            to: formattedPhone,
            message,
            appointmentId,
            status: "failed",
            error: err.message,
            timestamp: new Date().toISOString()
          });
        }
      }

      // Add to queue if not connected or failed
      const queueItem: any = {
        to: formattedPhone,
        message,
        appointmentId,
        type,
        status: "pending",
        attempts: 0,
        createdAt: new Date().toISOString()
      };
      
      const docId = await dbService.save("whatsapp_queue", queueItem);
      res.json({ status: "queued", id: docId });
    } catch (error: any) {
      console.error("Error queueing WhatsApp message:", error.message);
      res.status(500).json({ error: "Internal server error" });
    }
  });

  // WhatsApp Connection Endpoints
  app.get("/api/whatsapp/status", (req, res) => {
    const status = whatsappProvider.getStatus();
    res.json({
      ...status,
      quotaExhausted: dbService.isQuotaExhausted()
    });
  });

  app.post("/api/whatsapp/connect", async (req, res) => {
    console.log(`[API][WhatsApp] Connect solicitado.`);
    const status = whatsappProvider.getStatus();
    console.log(`[API][WhatsApp] Status atual: ${status.status}`);
    
    if (status.status === 'disconnected' || status.status === 'error') {
      console.log(`[API][WhatsApp] Iniciando provider...`);
      await whatsappProvider.init();
    }
    res.json(whatsappProvider.getStatus());
  });

  app.post("/api/whatsapp/sync", async (req, res) => {
    console.log(`[API][WhatsApp] Sincronização manual solicitada.`);
    try {
      await whatsappProvider.syncStatus();
      res.json(whatsappProvider.getStatus());
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post("/api/whatsapp/logout", async (req, res) => {
    console.log(`[API][WhatsApp] Logout solicitado pelo cliente.`);
    try {
      console.log(`[API][WhatsApp] Iniciando logout via provider...`);
      await whatsappProvider.logout();
      
      console.log(`[API][WhatsApp] Logout concluído com sucesso. Sessão limpa.`);
      res.json({ status: 'disconnected', message: 'Sessão encerrada e dados limpos.' });
    } catch (err: any) {
      console.error(`[API][WhatsApp] ERRO CRÍTICO DURANTE LOGOUT:`, err.message);
      res.status(500).json({ status: 'error', error: `Falha ao desconectar: ${err.message}` });
    }
  });

  app.post("/api/whatsapp/clear-session", async (req, res) => {
    console.log(`[API][WhatsApp] Limpeza manual de sessão solicitada.`);
    await whatsappProvider.clearSession();
    res.json({ status: 'cleared' });
  });

  // TEST ENDPOINT: Direct sending to validate integration
  app.get("/api/whatsapp/test-send", async (req, res) => {
    const { number, message } = req.query;
    
    if (!number || !message) {
      return res.status(400).json({ error: "Missing number or message query parameters. Use: /api/whatsapp/test-send?number=5516...&message=Hello" });
    }

    console.log(`[Uazapi][TEST] Manual test send requested for ${number}`);
    
    try {
      const result = await whatsappProvider.sendMessage(number as string, message as string);
      res.json({ success: true, result });
    } catch (err: any) {
      console.error(`[Uazapi][TEST] Manual test failed:`, err.message);
      res.status(500).json({ success: false, error: err.message });
    }
  });

  // TEST ENDPOINT: Comprehensive validation (Isolated from global state)
  app.get("/api/whatsapp/test-all", async (req, res) => {
    const { number } = req.query;
    console.log(`[UAZAPI][STATUS] Manual Test Mode Triggered. Send to: ${number || 'none'}`);
    
    try {
      const api = (whatsappProvider as any).ensureUazapi();
      
      // If number is provided, we perform a SEND test and return THAT diagnostic
      if (number) {
        const testMsg = `Teste WhatsApp Clínica Duque e Silva - Verificação Oficial`;
        const sendResult = await api.sendMessage(number as string, testMsg);
        
        return res.json({
          integration: "Uazapi",
          instance: "clinica1",
          actionType: "send",
          success: sendResult.success,
          diagnostic: sendResult, // THIS is now the send diagnostic
          status: "connected" // We assume connected for test purpose if logic reached here
        });
      }

      // 1. Otherwise, real remote status check
      const statusResult = await api.getStatus();

      res.json({
        integration: "Uazapi",
        instance: "clinica1", 
        actionType: "status",
        status: statusResult.status,
        diagnostic: statusResult
      });
    } catch (err: any) {
      console.error(`[UAZAPI][ERROR] Diagnostic test failed:`, err.message);
      res.status(500).json({ 
        error: err.message, 
        status: 'error',
        actionType: number ? "send" : "status"
      });
    }
  });

  // BUTTON INVESTIGATION ENDPOINT
  app.get("/api/whatsapp/test-buttons", async (req, res) => {
    const { number } = req.query;
    if (!number) return res.status(400).json({ error: "Missing number" });

    try {
      const api = (whatsappProvider as any).ensureUazapi();
      const result = await api.testButtons(number as string);
      
      res.json({
        integration: "Uazapi",
        instance: "clinica1",
        actionType: "send_buttons",
        success: result.success,
        diagnostic: result
      });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  // MENU/BOTÕES INVESTIGATION ENDPOINT (OFFICIAL /send/menu)
  app.get("/api/whatsapp/test-menu", async (req, res) => {
    const { number } = req.query;
    if (!number) return res.status(400).json({ error: "Missing number" });

    try {
      const api = (whatsappProvider as any).ensureUazapi();
      const result = await api.testMenu(number as string);
      
      res.json({
        integration: "Uazapi",
        instance: "clinica1",
        actionType: "send_menu",
        success: result.success,
        diagnostic: result
      });
    } catch (err: any) {
      console.error(`[UAZAPI][ERROR] Menu test failed:`, err.message);
      res.status(500).json({ 
        error: err.message,
        success: false,
        actionType: "send_menu"
      });
    }
  });

  // TEST REMINDER ENDPOINT (Manual test for 24h/3h flows)
  app.get("/api/whatsapp/test-reminder", async (req, res) => {
    const { number, type } = req.query;
    if (!number || !type) return res.status(400).json({ error: "Missing number or type" });
    if (type !== '24h' && type !== '3h') return res.status(400).json({ error: "Invalid type" });

    try {
      const api = (whatsappProvider as any).ensureUazapi();
      const result = await api.testReminder(number as string, type as '24h' | '3h');
      
      res.json({
        integration: "Uazapi",
        instance: "clinica1",
        actionType: `test_reminder_${type}`,
        success: result.success,
        diagnostic: result
      });
    } catch (err: any) {
      console.error(`[UAZAPI][ERROR] Reminder test failed:`, err.message);
      res.status(500).json({ 
        error: err.message,
        success: false,
        actionType: `test_reminder_${type}`
      });
    }
  });

  // MANUAL REMINDER JOB TRIGGER
  app.post("/api/whatsapp/run-reminders-job", async (req, res) => {
    try {
      const result = await automateReminders(true);
      res.json({
        ...result,
        requestUrl: "/api/whatsapp/run-reminders-job",
        method: "POST"
      });
    } catch (err: any) {
      res.status(500).json({ success: false, error: err.message, requestUrl: "/api/whatsapp/run-reminders-job" });
    }
  });

  // Interactive Confirmation Page Route
  app.get("/confirmar/:id", async (req, res) => {
    const { id } = req.params;
    const { n: name, d: date, t: time, p: prof } = req.query;

    res.send(`
      <!DOCTYPE html>
      <html lang="pt-BR">
      <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Confirmação de Consulta | Duque e Silva</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
        <style>
          body { font-family: 'Inter', sans-serif; }
          .btn-confirm { background: #10b981; }
          .btn-cancel { background: #ef4444; }
          .btn-reschedule { background: #6366f1; }
        </style>
      </head>
      <body class="bg-slate-50 min-h-screen flex items-center justify-center p-4">
        <div class="max-w-md w-full bg-white rounded-[2rem] shadow-2xl overflow-hidden border border-slate-100">
          <div class="bg-[#843951] p-8 text-center text-white">
            <div class="w-16 h-16 bg-white/20 rounded-2xl flex items-center justify-center mx-auto mb-4 backdrop-blur-sm">
              <svg xmlns="http://www.w3.org/2000/svg" class="h-8 w-8" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
              </svg>
            </div>
            <h1 class="text-xl font-bold">Confirmação de Consulta</h1>
            <p class="text-white/80 text-sm mt-1">Duque e Silva Odontologia</p>
          </div>
          
          <div class="p-8">
            <div class="space-y-4 mb-8">
              <div class="flex justify-between items-center pb-4 border-b border-slate-50">
                <span class="text-slate-400 text-sm">Paciente</span>
                <span class="text-slate-700 font-semibold">${name}</span>
              </div>
              <div class="flex justify-between items-center pb-4 border-b border-slate-50">
                <span class="text-slate-400 text-sm">Data</span>
                <span class="text-slate-700 font-semibold">${date}</span>
              </div>
              <div class="flex justify-between items-center pb-4 border-b border-slate-50">
                <span class="text-slate-400 text-sm">Horário</span>
                <span class="text-slate-700 font-semibold">${time}</span>
              </div>
              <div class="flex justify-between items-center">
                <span class="text-slate-400 text-sm">Profissional</span>
                <span class="text-slate-700 font-semibold">${prof}</span>
              </div>
            </div>

            <div class="space-y-3">
              <a href="/api/confirm-appointment?id=${id}&action=confirm&name=${encodeURIComponent(name as string)}&date=${encodeURIComponent(date as string)}&time=${encodeURIComponent(time as string)}&prof=${encodeURIComponent(prof as string)}" 
                 class="flex items-center justify-center gap-3 w-full py-4 btn-confirm text-white font-bold rounded-2xl shadow-lg shadow-emerald-200 hover:scale-[1.02] transition-transform">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
                  <path fill-rule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clip-rule="evenodd" />
                </svg>
                Confirmar Presença
              </a>
              
              <a href="/api/confirm-appointment?id=${id}&action=reschedule&name=${encodeURIComponent(name as string)}&date=${encodeURIComponent(date as string)}&time=${encodeURIComponent(time as string)}&prof=${encodeURIComponent(prof as string)}" 
                 class="flex items-center justify-center gap-3 w-full py-4 btn-reschedule text-white font-bold rounded-2xl shadow-lg shadow-indigo-200 hover:scale-[1.02] transition-transform">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
                  <path fill-rule="evenodd" d="M4 2a1 1 0 011 1v2.101a7.002 7.002 0 0111.601 2.566 1 1 0 11-1.885.666A5.002 5.002 0 005.999 7H9a1 1 0 010 2H4a1 1 0 01-1-1V3a1 1 0 011-1zm.008 9.057a1 1 0 011.276.61A5.002 5.002 0 0014.001 13H11a1 1 0 110-2h5a1 1 0 011 1v5a1 1 0 11-2 0v-2.101a7.002 7.002 0 01-11.601-2.566 1 1 0 01.61-1.276z" clip-rule="evenodd" />
                </svg>
                Reagendar
              </a>

              <a href="/api/confirm-appointment?id=${id}&action=cancel&name=${encodeURIComponent(name as string)}&date=${encodeURIComponent(date as string)}&time=${encodeURIComponent(time as string)}&prof=${encodeURIComponent(prof as string)}" 
                 class="flex items-center justify-center gap-3 w-full py-4 btn-cancel text-white font-bold rounded-2xl shadow-lg shadow-rose-200 hover:scale-[1.02] transition-transform">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
                  <path fill-rule="evenodd" d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z" clip-rule="evenodd" />
                </svg>
                Desmarcar
              </a>
            </div>
          </div>
        </div>
      </body>
      </html>
    `);
  });

  // Patient Confirmation Route
  app.get("/api/confirm-appointment", async (req, res) => {
    const { id, action, name, date, time, prof } = req.query;
    const clinicPhone = "5516992043095";

    if (!id || !action) {
      return res.status(400).send("Dados inválidos.");
    }

    const actionLabels: Record<string, string> = {
      confirm: "Confirmar Presença",
      cancel: "Desmarcar",
      reschedule: "Reagendar"
    };

    const selectedOption = actionLabels[action as string] || action;

    // 1. Update appointment status in Firestore based on action
    try {
      let newStatus: string = "";
      
      if (action === 'confirm') {
        newStatus = 'Confirmado';
      } else if (action === 'cancel') {
        newStatus = 'Pac. Cancelou';
      } else if (action === 'reschedule') {
        newStatus = 'Remarcado';
      }

      if (newStatus) {
        await dbService.update("agendamentos", id as string, { 
          status: newStatus,
          patientResponse: selectedOption,
          respondedAt: new Date().toISOString()
        });
      }

      // 2. Notify the clinic via WhatsApp
      const notificationMessage = `Paciente ${name} respondeu à confirmação da consulta de ${date} às ${time} com ${prof}. Opção selecionada: ${selectedOption}.`;
      
      const queueItem = {
        to: clinicPhone,
        message: notificationMessage,
        appointmentId: id,
        type: "update",
        status: "pending",
        attempts: 0,
        createdAt: new Date().toISOString()
      };

      await dbService.save("whatsapp_queue", queueItem);
      processWhatsAppQueue();

      // 3. Respond to patient
      res.send(`
        <!DOCTYPE html>
        <html lang="pt-BR">
        <head>
          <meta charset="UTF-8">
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <title>Confirmação de Consulta</title>
          <script src="https://cdn.tailwindcss.com"></script>
          <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
          <style>
            body { font-family: 'Inter', sans-serif; }
          </style>
        </head>
        <body class="bg-slate-50 flex items-center justify-center min-h-screen p-4">
          <div class="bg-white rounded-3xl shadow-xl p-8 max-w-md w-full text-center border border-slate-100">
            <div class="w-20 h-20 bg-emerald-100 text-emerald-600 rounded-full flex items-center justify-center mx-auto mb-6">
              <svg xmlns="http://www.w3.org/2000/svg" class="h-10 w-10" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="3" d="M5 13l4 4L19 7" />
              </svg>
            </div>
            <h1 class="text-2xl font-bold text-slate-800 mb-2">Obrigado, ${name}!</h1>
            <p class="text-slate-600 mb-8">
              Sua resposta de <strong>"${selectedOption}"</strong> foi registrada com sucesso e enviada para nossa equipe.
            </p>
            <div class="bg-slate-50 rounded-2xl p-4 text-left border border-slate-100 mb-8">
              <div class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Detalhes da Consulta</div>
              <div class="text-sm text-slate-700 space-y-1">
                <p><strong>Data:</strong> ${date}</p>
                <p><strong>Hora:</strong> ${time}</p>
                <p><strong>Profissional:</strong> ${prof}</p>
              </div>
            </div>
            <p class="text-xs text-slate-400">Você já pode fechar esta aba.</p>
          </div>
        </body>
        </html>
      `);
    } catch (error) {
      console.error("Error processing patient confirmation:", error);
      res.status(500).send("Ocorreu um erro ao processar sua resposta. Por favor, tente novamente mais tarde.");
    }
  });

  // WhatsApp Webhook - Real Integration for Uazapi
  app.post("/api/webhooks/whatsapp", (req, res) => {
    try {
      const payload = req.body;
      // console.log("[Webhook][Uazapi] Payload recebido:", JSON.stringify(payload));

      // Handle Uazapi / Evolution API style messages
      const isMessage = payload.event === 'messages.upsert' || payload.event === 'MESSAGES_UPSERT';
      
      if (isMessage && payload.data) {
        // Emit in a format compatible with existing listeners
        // Existing listener expects: { messages: [ { key: { remoteJid, fromMe }, message: { conversation } } ] }
        const messageData = payload.data;
        
        // normalize structure if it's nested differently
        const msg = messageData.message || messageData;
        
        whatsappProvider.emit('message', {
          messages: [ msg ]
        });
      }

      res.status(200).send("EVENT_RECEIVED");
    } catch (err: any) {
      console.error("[Webhook][ERROR] Falha ao processar evento:", err.message);
      res.status(500).send("INTERNAL_ERROR");
    }
  });

  // Vite middleware for development
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), 'dist');
    app.use(express.static(distPath));
    app.get('*', (req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  // Background workers
  setInterval(processWhatsAppQueue, 30000); // Every 30 seconds
  setInterval(automateInstantNotifications, 120000); // Every 2 minutes
  setInterval(automateReminders, 300000); // Every 5 minutes (for precise 3h window)
  
  // Peridic status sync for Uazapi
  setInterval(() => {
    if (whatsappProvider.getConnectionStatus() !== 'error' && !dbService.isQuotaExhausted()) {
      whatsappProvider.syncStatus();
    }
  }, 60000); // Check status every minute

  // Initial run after a delay to allow system stabilize
  setTimeout(() => {
    console.log('[Workers][Uazapi] Initial run after cooldown...');
    whatsappProvider.init(); // Start Uazapi check
    processWhatsAppQueue();
    automateInstantNotifications();
    automateReminders();
  }, 15000); // Wait 15 seconds before first automatic run (improved from 60s)

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });
}

startServer();
