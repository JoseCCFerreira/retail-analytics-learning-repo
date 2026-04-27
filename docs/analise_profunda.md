# Análise Profunda — Retail Analytics Study Case

## 1. Resumo executivo
Este repositório foi estruturado para servir simultaneamente três objetivos: aprendizagem técnica, demonstração profissional e base de implementação incremental. A documentação existente cobre o percurso para iniciantes e o roadmap orientado ao perfil Bosch. Com os novos artefactos adicionados, o projeto passa a ter um ponto único de entrada, roteiro de instalação completo e visão analítica aprofundada para tomada de decisão.

## 2. Escopo funcional e técnico
- **Domínio:** analytics de retalho.
- **Camada operacional:** SQLite para persistência transacional simplificada.
- **Camada analítica:** DuckDB para consulta e agregação local de alta velocidade.
- **Transformação governada:** dbt para modelação por camadas, testes e lineage.
- **Treino de lógica de BD enterprise:** scripts de conceitos PL/SQL (procedures, functions, triggers, packages).

### 2.1 Fluxo de dados pretendido
1. Geração de dados simulados (CSV).
2. Carga para SQLite (modelo operacional).
3. Carga/transformação para DuckDB (modelo analítico).
4. Curadoria em dbt (`staging` → `intermediate` → `marts`).
5. Consumo por consultas analíticas e dashboards.

## 3. Avaliação da documentação atual
### Pontos fortes
- Explicação amigável para iniciantes.
- Contexto de entrevista bem orientado a competências.
- Setup inicial já existente.

### Lacunas identificadas
- Falta de `requirements.txt` para instalação reproduzível.
- Ausência de `package.json` com scripts utilitários de execução de docs.
- Falta de documento técnico consolidado para decisão arquitetural.
- Necessidade de uma apresentação curta para comunicação executiva.

## 4. Entregáveis adicionados nesta iteração
- `index.html`: ponto central do repositório, com navegação por objetivos.
- `docs/apresentacao_curta.html`: versão resumida para pitch técnico.
- `docs/analise_profunda.md`: análise aprofundada (este documento).
- `docs/setup_and_installation.html`: guia atualizado, completo e corrigido.
- `requirements.txt`: dependências Python essenciais para execução do caso.
- `package.json`: metadados e scripts utilitários para abrir/servir documentação.

## 5. Dependências e racional
### Dependências Python propostas
- `pandas`: manipulação tabular.
- `duckdb`: motor analítico local.
- `sqlite-utils`: apoio à operação com SQLite.
- `dbt-core` + `dbt-duckdb`: camada de transformação e governança.
- `streamlit` + `plotly`: visualização e exploração.
- `faker`: geração de dados simulados.
- `pyarrow`: suporte a formatos colunares e integração de dados.

### Scripts de pacote
No `package.json`, os scripts definidos servem para:
- abrir documentação local com servidor HTTP simples;
- reduzir fricção no onboarding de quem só precisa consultar conteúdos.

## 6. Riscos técnicos e mitigação
1. **Divergência entre documentação e código real**
   - Mitigação: checklist de validação após cada execução local.
2. **Dependências com versões incompatíveis**
   - Mitigação: pin de versões mínimas e revisão trimestral.
3. **Falta de dados de teste consistentes**
   - Mitigação: geração determinística com sementes e snapshots.
4. **Complexidade crescente no dbt**
   - Mitigação: convenções de naming e testes por camada.

## 7. Métricas de sucesso recomendadas
- **Onboarding técnico:** setup completo em menos de 30 minutos.
- **Qualidade de transformação:** 100% dos testes críticos dbt em verde.
- **Confiabilidade documental:** 0 comandos quebrados no guia principal.
- **Comunicabilidade:** apresentação curta compreendida por público não técnico em até 5 minutos.

## 8. Plano de evolução (30/60/90 dias)
### 30 dias
- Validar execução ponta-a-ponta em máquina limpa.
- Criar script único de bootstrap.
- Adicionar validação automática de links/documentação.

### 60 dias
- Introduzir dados incrementais e cenários de late-arriving facts.
- Adicionar testes de qualidade para regras de negócio críticas.
- Publicar dicionário de dados em formato acessível.

### 90 dias
- Integrar orchestrator (ex.: Airflow/GitHub Actions).
- Adicionar monitorização de freshness e SLA de dados.
- Evoluir documentação para playbook operacional.

## 9. Conclusão
O projeto fica preparado para uso real de estudo, apresentação e expansão técnica. A combinação de documentação em três níveis (iniciante, executiva e profunda) com setup reproduzível reduz risco de adoção, melhora a narrativa profissional e cria base sólida para evolução incremental do repositório.
