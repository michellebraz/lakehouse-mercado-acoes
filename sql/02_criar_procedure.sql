-- 02_criar_procedure.sql (rodar sozinho, depois do 01)
CREATE OR ALTER PROCEDURE dbo.usp_atualiza_watermark
  @tabela VARCHAR(100),
  @novo DATETIME2
AS
BEGIN
  SET NOCOUNT ON;
  UPDATE dbo.controle_watermark SET ultimo_valor = @novo WHERE tabela = @tabela;
END;
