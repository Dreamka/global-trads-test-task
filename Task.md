### Условия задачи

**Job должен:**

- найти клиента по номеру телефона;
- выбрать доступного оператора;
- назначить звонок оператору;
- отправить событие в телефонию;
- записать лог;
- при ошибке повториться.

Система работает в production под нагрузкой. Обработка звонков выполняется несколькими воркерами параллельно.

```php
class ProcessIncomingCallJob implements ShouldQueue  
{
    public $tries = 5;
    private $callId;
    
    public function __construct($callId)
    {
        $this->callId = $callId;
    }
    
    public function handle()
    {
        $call = Call::find($this->callId);
  
        if (!$call) {
            return;
        }
  
        if ($call->status === 'new') {
            $client = Client::where('phone', $call->phone)->first();

            if ($client) {
                $call->client_id = $client->id;
            }

            $operator = Operator::where('available', true)->orderBy('last_call_at')->first();
  
            if (!$operator) {
                throw new \Exception('No available operators');
            }

            $operator->available = false;
            $operator->save();
            $call->operator_id = $operator->id;
            $call->status = 'assigned';
            $call->save();

			// HTTP-запрос во внешнюю телефонию для назначения звонка оператору.
			// Гарантии внешней системы неизвестны.
			app(TelephonyClient::class)->sendCallAssigned($call->id, $operator->id);
			
            Log::info(
	            'Call assigned',
	            [
		            'call_id' => $call->id,
		            'operator_id' => $operator->id,
	            ]);
        }
    }
}
```  

### Проблемы

1. **Не обновляется `last_call_at`** - ломается ротация операторов по `orderBy('last_call_at')`.
2. **При работе нескольких воркеров почти наверняка возникнут гонки условий** - два воркера могут выбрать одного оператора, пока оба видят `available = true`.  Воркеры могут одновременно назначить одному оператору несколько звонков, считав статус available до того как один из воркеров назначит ему available=false.
3. **Нет атомарности** - оператор может зависнуть без звонка если `$call->save()`; упадет, потому что статус оператора уже изменен, звонок ему не назначен, телефония либо не уведомлена, либо получила невалидные данные.
4. **Телефония без идемпотентности** - при падении `sendCallAssigned` звонок уже не `new`, а это значит, что повторное назначение не произойдёт.
5. **Нет защиты от дублей джобы** - один звонок может обрабатываться параллельно несколькими воркерами.

### Предлагаемое решение

Либо создаем триггер в БД, либо реализуем в коде обновление `last_call_at`.

```php  
$operator->update(['available' => false, 'last_call_at' => now()]);  
```

Добавить транзакцию для блокировок операторов и звонков и разделить процесс на два этапа:

1. **В транзакции** - найти и заблокировать оператора (`lockForUpdate`), назначить звонок, перевести в промежуточный статус.
2. **Вне транзакции** - уведомить телефонию, зафиксировать результат.

Цепочка статусов будет примерно такая: `new` -> `assigned_pending_telephony` -> `assigned` / `failed`.

Так же неплохо будет добавить:
- `telephony_notified_at` - флаг успешного уведомления;
- `telephony_idempotency_key` - ключ идемпотентности запросов в телефонию;
- `WithoutOverlapping` - защита от параллельного запуска джобы на один звонок.

### Поведение при ошибках

**Нет свободных операторов** - бросаем `NoAvailableOperatorsException`. $tries можно заменить на  `release(n)`.

**Сбои звонков** если оператор успел взять другой звонок между повторами, `operator_id` в звонке может устареть. Это решается отдельным статусом `failed`, полем `telephony_error` или повторным назначением.

**Внешняя система**
Так как мы не знаем как ведет себя внешняя система, следует более тщательно подходить к отправке звонка

```php  
app(TelephonyClient::class)->sendCallAssigned($call->id, $operator->id);  
```  

Здесь нет никаких проверок или исключений которые мы можем отработать при падении sendCallAssigned или получении некорректного ответа. В приведенном примере из условия задачи воркер может снять со звонка статус new и при падении sendCallAssigned мы получим зависший звонок.

Необходимо добавить поля `telephony_notified_at` как флаг уведомления телефонии и `telephony_idempotency_key` для идемпотентности уведомлений

```php  
Schema::table('calls', function (Blueprint $table) {  
    $table->timestamp('telephony_notified_at')->nullable()->after('status');
    $table->string('telephony_idempotency_key', 64)->nullable()->unique();
});  
```  

А так же индекс на операторах: `(available, last_call_at, id)`.

**Падение телефонии** - освобождаем оператора, бросаем исключение, джоба повторится. При повторе звонок будет уже в статусе `assigned_pending_telephony` с ключем идемпотентности, повторного назначения не будет - только повторный вызов телефонии с тем же ключом.

Контракт `TelephonyClient::sendCallAssigned` должен возвращать результат и принимать idempotency key. Таймаут 3–5 с, логирование ответов и ошибок. Затем разделить этапы уведомления

### Итоговый воркер

```php  
class ProcessIncomingCallJob implements ShouldQueue
{
    public $tries = 5;
    private $callId;
    
    public function __construct($callId)
    {
        $this->callId = $callId;
    }
  
    public function middleware(): array
    {
        return [
            (new WithoutOverlapping("call:{$this->callId}"))
                ->releaseAfter(10)
                ->expireAfter(180)
        ];
    }
  
    public function handle()
    {  
        $call = Call::find($this->callId);
        
        if (!$call || $call->telephony_notified_at !== null) {
            return;
        }
        
        if ($call->status === 'new') {
            $client = Client::where('phone', $call->phone)->first(); 
            
            if ($client) {
                $call->update(['client_id' => $client->id]);
            }
        }
        
        if (!in_array($call->status, ['new', 'assigned_pending_telephony'], true)) {
            return;
        }
        
        DB::transaction(function () {
            $call = Call::query()
                ->whereKey($this->callId)
                ->lockForUpdate()
                ->firstOrFail();
                
            if ($call->telephony_notified_at !== null) {
                return;
            };
            
            if ($call->status === 'assigned_pending_telephony') {
                return;
            }
            
            $operator = Operator::where('available', true)
                ->orderBy('last_call_at')
                ->orderBy('id')
                ->lockForUpdate()
                ->first();
                
            if (!$operator) {
                throw new NoAvailableOperatorsException();
            }
            
            $operator->update([
	            'available' => false,
	            'last_call_at' => now()
	        ]);

            $call->update([
                'operator_id' => $operator->id,
                'status' => 'assigned_pending_telephony',
                'telephony_idempotency_key' => (string)Str::uuid(),
            ]);
        });
        
        $call->refresh();  
        
        if ($call->telephony_notified_at !== null || !$call->operator_id) {  
            return;
        }  
                  
        $operator = Operator::findOrFail($call->operator_id);
        
        $sendedCall = app(TelephonyClient::class)
	        ->sendCallAssigned(
		        $call->id,
		        $operator->id,
		        $call->telephony_idempotency_key
		    );  
		    
        if ($sendedCall) {
            Call::query()->whereKey($this->callId)
                ->whereNull('telephony_notified_at')
                ->update([
                        'telephony_notified_at' => now(),
                        'status' => 'assigned'
                    ]);
                    
            Log::info(
	            'Call assigned',
	            [
		            'call_id' => $call->id,
		            'operator_id' => $operator->id
		        ]);
        } else {  
            $operator->update(['available' => true]);
            
            Log::error(
                'ProcessIncomingCallJob failed',
                [
                    'call_id' => $this->callId,
                    'operator_id' => $operator->id
                ]);
                
            throw new \RuntimeException('Telephony notification failed');  
        }  
    }  
}
```  

Это не финальный, но рабочий вариант - дальше надо разносить логику и отвественность по сервисам и другим сущностям в соответствии с принципами SOLID. В рамкаж тестового - работаю с приведенным кодом

### Защита от дублей
**Неплохо было бы каждую джобу сделать уникальной в качестве дополнительной защиты от дублей**

Например так

```php  
class ProcessIncomingCallJob implements ShouldQueue, ShouldBeUnique  
{
	public int $uniqueFor = 300;
	public function uniqueId(): string
	{
		return 'incoming-call:' . $this->callId;
	}
}  
  
ProcessIncomingCallJob::dispatch($call->id)->onQueue('incoming-calls');  
```  

И добавить middleWare к джобе

```php  
public function middleware(): array  
{
	return [
		(new WithoutOverlapping("call:{$this->callId}"))
		->releaseAfter(10)
		->expireAfter(180),
	];
}  
```  

В итоге получится три уровня защиты джобы. Идемпотентность в БД, уникальный идентификатор, и защита от повторных запусков.

### Что можно добавить
- нормализация номера телефона;
- `failed()` при исчерпании попыток или ответа оператора на другой звонок;
- мониторинг зависших звонков в `assigned_pending_telephony`;
- разделение на джобы: назначение -> телефония -> обработка ошибок;
- отдельные очереди `assign` / `telephony` / `retry`.

### Тесты
- два звонка не назначаются одному оператору параллельно;
- дубли джобы на один `callId` не ломают состояние;
- нет свободных операторов - корректная ошибка / отложенный повтор;
- повтор `sendCallAssigned` после падения - идемпотентность по ключу;
- падение `save()` внутри транзакции - откат, оператор остаётся доступным.

Простое увеличение числа воркеров помогает пока мы не упираемся в лимиты в БД, телефонии или Redis. Дальше - метрики и инфраструктура.
### Слабое место - внешняя система
Когда мы не знаем насколько система стабильна, мы не можем ей доверять в полной мере, поэтому надо покрывать все возможные варианты ответов, ошибок, обрывов коннектов и кривых данных. Наша система не должна падать при падении внешних систем, но должна писать лог, по которому можно восстановить хронологию и расследовать инцидент

### Масштабирование
Возможные проблемы при росте нагрузки и кол-ва воркеров:
- на каждый звонок воркеры блокируют (`lockForUpdate`) операторов для назначения. При росте кол-ва воркеров возможен рост задержки назначения оператора
- операторы выбираются по `orderBy(last_call_at)` а это значит что все воркеры будут пытаться занять одних и тех же операторов. Как следствие - неравномерное распределение нагрузки на операторов. Увеличение SLA ответов
- Блокировка звонков при выборе (`lockForUpdate`) скорее всего так же будет фактором увеличения SLA ответов.
- Отсутствие индексов увеличивает время выборки.
- При увеличении кол-ва воркеров возможен выход за лимиты соединений с БД и как следствие - недоступность БД.
- Экспоненциальный рост потребления памяти Redis из за роста кол-ва звонков
- Синхронный запрос в телефонию - занимает воркер и не дает ему работать по другим звонкам. Как следствие исчерпание воркеров и рост очереди звонков.
- Исчерпание лимитов внешней системы. При  большом кол-ве звонков генерируется большое кол-во внешних запросов. Внешняя система может начать отдавать 429 Too Many Requests
- Падение внешней системы - накапливает очередь запросов и при восстановлении на нее снова обрушивается шквал запросов.
- Исчерпание дисков логами. Кол-во генерирукемых логов растет вместе с кол-вом воркеров и звонков

Простое увеличение кол-ва воркеров поможет быстрее обрабатывать очередь пока не вышли за лимиты БД или Телефонии, однако при достижении лимитов в БД, памяти (Redis), CPU или получении 429 от телефонии увеличение количества воркеров перестает иметь хоть какое-то значение.

### Процесс масштабирования

1. Вынос логов в ELK / Grafana
2. Сбор метрик.
    - Размер очереди
    - Время работы воркеров
    - Время ответа телефонии
    - Кол-во коннектов в БД
    - Кол-во коннектов в Телефонии
    - % ошибок от телефонии
    - SLA назначения оператора
    - % пропущеных / неуспешных звонков

3. Индекс `(available, last_call_at, id)` на таблице операторов. Быстрее выборка, время работы воркера снижается
4. Разделение очередей `assign` / `telephony` / `retry`
5. Разделение процесса на несколько джоб. Например: Назначение оператора > Уведомление телефонии > Обработка ошибок
6. Разрыв соединения с телефонией по таймауту 3-5с + повтор.
7. Переиспользование коннектов в БД, например PgBouncer / ProxySQL. Снизит нагрузку на БД и вероятность выходов за лимиты, баны в БД
8. Отдельный инстанс Redis для только для очередей. Изолируем очереди от кешей и сессий