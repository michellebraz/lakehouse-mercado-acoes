-- 03_simular_alteracoes.sql
-- Rode depois da primeira carga do ADF para testar a carga incremental.
UPDATE dbo.ativos SET setor = N'Varejo e e-commerce', updated_at = SYSUTCDATETIME() WHERE ticker = 'MGLU3';
UPDATE dbo.posicoes SET quantidade = quantidade + 100, updated_at = SYSUTCDATETIME() WHERE cliente_id IN (1, 2, 3);
INSERT INTO dbo.clientes (cliente_id, nome, cidade, uf, perfil_investidor, data_cadastro)
VALUES (201, N'Cliente Novo', N'Santos', 'SP', N'Moderado', CAST(SYSUTCDATETIME() AS DATE));
-- Esperado na próxima execução do pipeline: 1 ativo, 1 cliente e 3 posições copiados.
