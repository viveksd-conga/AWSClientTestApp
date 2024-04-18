using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.AspNetCore.Mvc;
using Microsoft.VisualBasic;
using System.Data;
using System.IO;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace AWSClientTestApp.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class S3TestController : ControllerBase
    {
        private readonly ILogger<S3TestController> logger;
        private AmazonS3Client AmazonS3Client { get; set; }

        public S3TestController(ILogger<S3TestController> logger)
        {
            this.logger = logger;
            var s3Config = new AmazonS3Config() { ServiceURL = "https://s3.us-west-2.amazonaws.com" };
            AmazonS3Client = new AmazonS3Client("xxxx", "yyy", s3Config);
        }

        // GET: api/<S3TestController>
        [HttpGet("/S3BucketExistsTest/{bucketName}")]
        public async Task<IActionResult> S3BucketExistsTest(string bucketName)
        {
            try
            {
                var (bucketExists, keys) = await BucketExists(bucketName);
                if(bucketExists)
                {
                    if(keys== null || keys.Count== 0)
                    {
                        return Ok("Bucket exists but it's empty");
                    }
                    return Ok(keys);
                }
                throw new Exception("Bucket doesn't exist");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, ex.Message);
                throw;
            }
        }

        [HttpPost("/S3CreateTest/{bucketName}")]
        public async Task<IActionResult> S3CreateTest(string bucketName)
        {
            try
            {
                return Ok(await CreateBucket(bucketName));
                //return Ok(await CreateBucketV2(bucketName));
            }
            catch (Exception ex)
            {
                logger.LogError(ex, ex.Message);
                throw;
            }
        }
        [HttpPost("/CreateDeletionRules1Time")]
        public async Task<IActionResult> DeleteRulesPolicyCreation(string bucketName)
        {
            var configurationRequest = new GetLifecycleConfigurationRequest
            {
                BucketName = bucketName
            };

            var configurationResponse = await this.AmazonS3Client.GetLifecycleConfigurationAsync(configurationRequest);
            if(configurationResponse.Configuration.Rules.Count >= 1000)
            {
                throw new Exception("MAXIMUM HARD LIMIT [1000] LIFECYCLE RULES HAVE BEEN CREATED ALREADY");
            }

            this.logger.LogInformation($"Trying to fetch the Bucket : {bucketName}");
            var (doesBucketExists, prefixes) = await BucketExists(bucketName);
            bool tenant1to300 = false;
            int noOfRulesApplied1to300Tenants = 0;
            bool tenant301to600 = false;
            int noOfRulesApplied301to600Tenants = 0;
            bool tenant601to1000 = false;
            int noOfRulesApplied601to1000Tenants = 0;
            int noOfRulesApplied = configurationResponse.Configuration.Rules.Count;
            List<string> alreadyCreatedRulesPrefixes = new List<string>() { "tenant-10/tenant-10.txt" };
            foreach (var prefix in prefixes)
            {
                if (!(prefix == "tenant-10/tenant-10.txt"))
                {
                    var tenantName = prefix.Split('/')[0];
                    var existingRules = configurationResponse.Configuration.Rules.Select(x => x.Id).ToList();
                    var ruleIdCleanup = string.Format(S3AutoDeletePolicy.CLEANUP_DATA_LIFECYCLE_RULE_NAME, bucketName, tenantName); 
                    var ruleIdMarker = string.Format(S3AutoDeletePolicy.REMOVE_DELETEMARKER_LIFECYCLE_RULE_NAME, bucketName, tenantName); 
                    if(existingRules.Contains(ruleIdCleanup) && existingRules.Contains(ruleIdMarker))
                    {
                        alreadyCreatedRulesPrefixes.Add(prefix);
                    }
                }
            }
            if (doesBucketExists)
            {
                logger.LogInformation($"Found the Bucket : {bucketName}");
                var mainStartTime = DateTime.UtcNow;
                this.logger.LogInformation($"Starting the creation of Cleanup and Delete Marker Rules for bucket : {bucketName} at : {mainStartTime}");
                foreach (var prefix in prefixes)
                {
                    if(noOfRulesApplied <= 1000 && !alreadyCreatedRulesPrefixes.Contains(prefix))
                    {
                        var stringManupulation = prefix.Split('/');
                        var tenantName = stringManupulation[stringManupulation.Length - 1];
                        var splitTenantNumber = tenantName.Split('-');
                        var tenantNumber = int.Parse(splitTenantNumber[1]);
                        if (tenantNumber >= 1 && tenantNumber <= 300)
                        {
                            var startTime = DateTime.UtcNow;
                            this.logger.LogInformation($"Creating cleanup and deletion rule policy for {tenantName} at TimeStamp : {startTime}");
                            await CreateBucketDataDeletePolicy(bucketName, prefix, 1);
                            var endTime = DateTime.UtcNow;
                            this.logger.LogInformation($"Successfully created cleanup and deletion rule policy for {tenantName} at TimeStamp : {endTime}");
                            this.logger.LogInformation($"Total Time Taken for creation of cleanup and deletion rules for {tenantName} : {endTime - startTime}");
                            noOfRulesApplied1to300Tenants += 2;
                            tenant1to300 = true;
                        }
                        else if (tenantNumber >= 301 && tenantNumber <= 600 && !alreadyCreatedRulesPrefixes.Contains(prefix))
                        {
                            var startTime = DateTime.UtcNow;
                            this.logger.LogInformation($"Creating cleanup and deletion rule policy for {tenantName} at TimeStamp : {startTime}");
                            await CreateBucketDataDeletePolicy(bucketName, prefix, 2);
                            var endTime = DateTime.UtcNow;
                            this.logger.LogInformation($"Successfully created cleanup and deletion rule policy for {tenantName} at TimeStamp : {endTime}");
                            this.logger.LogInformation($"Total Time Taken for creation of cleanup and deletion rules for {tenantName} : {endTime - startTime}");
                            noOfRulesApplied301to600Tenants += 2;
                            tenant301to600 = true;
                        }
                        else if(!alreadyCreatedRulesPrefixes.Contains(prefix))
                        {
                            var startTime = DateTime.UtcNow;
                            this.logger.LogInformation($"Creating cleanup and deletion rule policy for {tenantName} at TimeStamp : {startTime}");
                            await CreateBucketDataDeletePolicy(bucketName, prefix, 3);
                            var endTime = DateTime.UtcNow;
                            this.logger.LogInformation($"Successfully created cleanup and deletion rule policy for {tenantName} at TimeStamp : {endTime}");
                            this.logger.LogInformation($"Total Time Taken for creation of cleanup and deletion rules for {tenantName} : {endTime - startTime}");
                            noOfRulesApplied601to1000Tenants += 2;
                            tenant601to1000 = true;
                        }
                        noOfRulesApplied += 2;
                    }
                    else
                    {
                        if (noOfRulesApplied >= 1000)
                        {
                            this.logger.LogInformation($"MAX LIFECYCLE HARD LIMIT 1000 REACHED, HENCE STOPPING THE CREATION OF RULES");
                            break;
                        }
                        else
                        {
                            this.logger.LogInformation($"Cleanup and Marker Rule already created for {prefix} hence skipping.");
                            continue;
                        }
                    }
                }
                var mainEndTime = DateTime.UtcNow;
                var totalMainTime = mainStartTime - mainEndTime;
                this.logger.LogInformation($"Complted the creation of Cleanup and Delete Marker Rules for bucket : {bucketName} at : {mainEndTime}");
                this.logger.LogInformation($"Total Time Taken for the creation of Cleanup and Delete Marker Rules for bucket : {bucketName} : {totalMainTime}");
                return Ok($"Successfully created Cleanup and Delete Marker Rules for bucket : {bucketName} \n Total Time taken : {totalMainTime} \n Deletion Rules created for tenants 1 to 300 : {tenant1to300} , {noOfRulesApplied1to300Tenants} rules applied \n " +
                    $"Deletion Rules created for tenants 301 to 600 : {tenant301to600} , {noOfRulesApplied301to600Tenants} rules applied \n Deletion Rules created for tenants 600 to 1000 : {tenant601to1000} , {noOfRulesApplied601to1000Tenants} rules applied.");
            }
            else
            {
                throw new Exception("Bucket doesn't exist");
            }
        }

        [HttpPost("/S3FileUpload/{bucketName}/{folderPath}/{fileName}")]
        public async Task<IActionResult> S3FileUpload(string bucketName, string folderPath, IFormFile file)
        {
            var fileName = file.FileName;
            byte[] fileContent;
            using (MemoryStream memoryStream = new())
            {
                await file.CopyToAsync(memoryStream);
                fileContent = memoryStream.ToArray();
            }

            await UploadContent(bucketName, folderPath, fileName, fileContent);

            return Ok(bucketName + "/" + folderPath + "/" + fileName);
        }
        [HttpPost("/S3DeleteLifecycleRules")]
        public async Task<IActionResult> S3DeleteLifeycleRules(string bucket, string tenantName)
        {
            var (bucketExists, prefixes) = await BucketExists(bucket);
            if(bucketExists)
            {
                LifecycleConfiguration lifecycleConfiguration;
                var configurationRequest = new GetLifecycleConfigurationRequest
                {
                    BucketName = bucket
                };

                var configurationResponse = await this.AmazonS3Client.GetLifecycleConfigurationAsync(configurationRequest);
                if (configurationResponse.Configuration != null)
                {
                    lifecycleConfiguration = configurationResponse.Configuration;
                }
                else
                {
                    return (NotFound("Lifecycle Configuration doesn't exist for the given bucket"));
                }
                var cleanUpRuleName = string.Format(S3AutoDeletePolicy.CLEANUP_DATA_LIFECYCLE_RULE_NAME, bucket, tenantName);
                var deleteMarkerRuleName = string.Format(S3AutoDeletePolicy.REMOVE_DELETEMARKER_LIFECYCLE_RULE_NAME, bucket, tenantName);
                var cleanUpRule = lifecycleConfiguration.Rules.FirstOrDefault(x => x.Id == cleanUpRuleName);
                var deleteMarkerRule = lifecycleConfiguration.Rules.FirstOrDefault(x => x.Id == deleteMarkerRuleName);

                PutLifecycleConfigurationRequest putRequest = new PutLifecycleConfigurationRequest
                {
                    BucketName = bucket,
                    Configuration = lifecycleConfiguration
                };
                try
                {
                    var updateResponse = await this.AmazonS3Client.PutLifecycleConfigurationAsync(putRequest);
                    if (updateResponse.HttpStatusCode == System.Net.HttpStatusCode.OK)
                    {
                        return (Ok("Rules have been removed"));
                    }
                    else
                    {
                        return (BadRequest("Failed to delete lifecycle rules"));
                    }
                }
                catch(AmazonS3Exception s3Exception)
                {
                    this.logger.LogError(s3Exception, s3Exception.Message);
                    throw;
                }
            }
            else
            {
                return (this.BadRequest("Bucket doesn't Exist"));
            }
        }

        [HttpPost("/S3CreateDeleteRule/{bucket}/{folderpath}")]
        public async Task<IActionResult> S3CreateDeleteRuleFolderLevel(string bucket, string folderpath = "")
        {
            var (bucketExists, bucketKeys) = await BucketExists(bucket);
            bool isDeleteDataPolicyApplied;

            if (string.IsNullOrEmpty(folderpath))
            {
                isDeleteDataPolicyApplied = await CreateBucketDataDeletePolicy(bucket);
            }
            else
            {
                isDeleteDataPolicyApplied = await CreateBucketDataDeletePolicy(bucket, folderpath);
            }

            return Ok(isDeleteDataPolicyApplied);
        }


        [HttpPost("/Upload1000Folders")]
        public async Task<IActionResult> S3Upload1000FoldersWithFile(string bucketName)
        {
            var (doesBucketExist, objects) = await BucketExists(bucketName);
            const int noOfFolders = 1000;
            if (doesBucketExist)
            {
                for (int i = 1 ; i <= noOfFolders; i++)
                {
                    var fileName = $"tenant-{i}";
                    var folderPath = $"tenant-{i}";
                    string fileContentCreation = $"tenant-{i}";
                    byte[] fileContentBytes = Encoding.UTF8.GetBytes(fileContentCreation);
                    await UploadContent(bucketName, folderPath, fileName, fileContentBytes);
                }
                return Ok("Folders have been upload with a txt file for each");
            }
            else
            {
                return BadRequest("Bucket Doesn't exist");
            }
        }

        private async Task<(bool, List<string>?)> BucketExists(string bucketName)
        {
            try
            {
                var objectListResponse = await AmazonS3Client.ListObjectsAsync(bucketName);

                if (objectListResponse.HttpStatusCode == System.Net.HttpStatusCode.OK)
                {
                    var objects =  objectListResponse.S3Objects.Select(i => i.Key)?.ToList();
                    return (true, objects);
                }
                else
                {
                    this.logger.LogError($"Error getting bucket :{bucketName} ,{objectListResponse.HttpStatusCode} - {objectListResponse}");
                    return (false, null);
                }
            }
            catch (AmazonS3Exception s3Exception)
            {
                //StatusCode as BadRequest  and ErroCode : InvalidBucketName : invalid name
                //StatusCode as Conflict  and ErroCode : BucketAlreadyExists : someone elase has that bucket name
                //StatusCode as Conflict  and ErroCode : BucketAlreadyOwnedByYou : we have same bucket name in our account

                //bucket already exists
                //if anything else, throw exception
                this.logger.LogError(s3Exception, s3Exception.Message);
                throw;
            }
        }

        private async Task<bool> CreateBucketV2(string bucketName)
        {
            try
            {
                var bucketCreationResponse = await AmazonS3Client.PutBucketAsync(bucketName);
                if(bucketCreationResponse.HttpStatusCode == System.Net.HttpStatusCode.OK)
                {
                    return true;
                }
                return false;
            }
            catch(AmazonS3Exception s3Exception)
            {
                return false;
            }
        }
        private async Task<bool> CreateBucket(string bucketName)
        {
            //try to create bucket and handle any duplicate error
            //TO-DO , as we already throw exception for commercial , thus here only enterprise should come,but for now lets try to create whatever excepted bucket should be

            try
            {
                var bucketCreationResponse = await AmazonS3Client.PutBucketAsync(bucketName);

                if (bucketCreationResponse.HttpStatusCode == System.Net.HttpStatusCode.OK)
                {
                    //---bucket encryption 
                    var encryptionResponse = await EncryptBucket(bucketName);

                    if (encryptionResponse.HttpStatusCode == System.Net.HttpStatusCode.OK)
                    {
                        var versioningResponse = await EnableVersioning(bucketName);

                        // File auto delete policy for specific prefix and tag
                        if (versioningResponse.HttpStatusCode == System.Net.HttpStatusCode.OK)
                        {
                            return await CreateFileAutoDeletePolicy(bucketName, 1);
                        }
                        else
                        {
                            this.logger.LogError($"Error versioning bucket :{bucketName} ,{versioningResponse.HttpStatusCode} - {versioningResponse}");
                            return false;
                        }
                    }
                    else
                    {
                        this.logger.LogError($"Error encrypting bucket :{bucketName} ,{encryptionResponse.HttpStatusCode} - {encryptionResponse}");
                        return false;
                    }


                }
                else
                {
                    this.logger.LogError($"Error creating bucket :{bucketName} ,{bucketCreationResponse.HttpStatusCode} - {bucketCreationResponse}");
                    return false;
                }
            }
            catch (AmazonS3Exception s3Exception)
            {
                //StatusCode as BadRequest  and ErroCode : InvalidBucketName : invalid name
                //StatusCode as Conflict  and ErroCode : BucketAlreadyExists : someone elase has that bucket name
                //StatusCode as Conflict  and ErroCode : BucketAlreadyOwnedByYou : we have same bucket name in our account

                //bucket already exists
                if (s3Exception.StatusCode == System.Net.HttpStatusCode.Conflict
                    && s3Exception.ErrorCode == "BucketAlreadyOwnedByYou")
                {
                    this.logger.LogInformation($"Great ! BucketAlreadyOwnedByYou bucket:{bucketName}");
                    return true;
                }
                else
                {
                    //if anything else, throw exception
                    this.logger.LogError(s3Exception, s3Exception.Message);
                    throw;
                }
            }
        }

        private async Task UploadContent(string bucketName, string folderPath, string fileName, byte[] fileContent)
        {
            try
            {
                using (var newMemoryStream = new MemoryStream(fileContent))
                {
                    var putObjectRequest = new PutObjectRequest
                    {
                        InputStream = newMemoryStream,
                        Key = fileName,
                        BucketName = $"{bucketName}/{folderPath}"
                    };
                    await AmazonS3Client.PutObjectAsync(putObjectRequest);
                }
            }
            catch (AmazonS3Exception s3Exception)
            {
                //StatusCode as BadRequest  and ErroCode : InvalidBucketName : invalid name
                //StatusCode as Conflict  and ErroCode : BucketAlreadyExists : someone elase has that bucket name
                //StatusCode as Conflict  and ErroCode : BucketAlreadyOwnedByYou : we have same bucket name in our account

                //bucket already exists
                //if anything else, throw exception
                this.logger.LogError(s3Exception, s3Exception.Message);
                throw;
            }
        }

        private async Task<PutBucketVersioningResponse> EnableVersioning(string bucketName)
        {
            PutBucketVersioningRequest putBucketVersioningRequest = new PutBucketVersioningRequest()
            {
                BucketName = bucketName,
                VersioningConfig = new S3BucketVersioningConfig()
                {
                    Status = VersionStatus.Enabled
                }
            };
            return await AmazonS3Client.PutBucketVersioningAsync(putBucketVersioningRequest);
        }

        private async Task<PutBucketEncryptionResponse> EncryptBucket(string bucketName)
        {
            var serverSideEncryptionByDefault = new ServerSideEncryptionByDefault();
            serverSideEncryptionByDefault.ServerSideEncryptionAlgorithm = ServerSideEncryptionMethod.AES256;

            ServerSideEncryptionRule serverSideEncryptionRule = new ServerSideEncryptionRule();
            serverSideEncryptionRule.ServerSideEncryptionByDefault = serverSideEncryptionByDefault;

            ServerSideEncryptionConfiguration serverSideEncryptionConfiguration = new ServerSideEncryptionConfiguration();
            serverSideEncryptionConfiguration.ServerSideEncryptionRules.Add(serverSideEncryptionRule);

            PutBucketEncryptionRequest putBucketEncryptionRequest = new PutBucketEncryptionRequest()
            {
                BucketName = bucketName,
                ServerSideEncryptionConfiguration = serverSideEncryptionConfiguration
            };

            var putBucketEncryptionResponse = await AmazonS3Client.PutBucketEncryptionAsync(putBucketEncryptionRequest);

            return putBucketEncryptionResponse;
        }

        private async Task<bool> CreateFileAutoDeletePolicy(string bucketName, int days)
        {
            this.logger.LogInformation($"Trying to check if Auto Delete policy exists.");

            var bucketPolicyExists = await CheckBucketPolicyExists(bucketName);
            if (!bucketPolicyExists)
            {
                this.logger.LogInformation($"Bucket policy does not exist, create policy.");
                return await CreateBucketPolicy(bucketName, days);
            }
            this.logger.LogInformation($"CreateFileAutoDeletePolicy finished");
            return true; // bucket policy already exists
        }

        private async Task<bool> CheckBucketPolicyExists(string bucketName)
        {
            // Retrieve current configuration
            var getRequest = new GetLifecycleConfigurationRequest
            {
                BucketName = bucketName
            };

            LifecycleConfiguration configuration = (await AmazonS3Client.GetLifecycleConfigurationAsync(getRequest)).Configuration;

            if (configuration != null && configuration.Rules != null
                && configuration.Rules.Any(r => r.Id == S3AutoDeletePolicy.ONE_DAY_LIFECYCLE_RULE_NAME))
            {
                this.logger.LogInformation($"File auto delete rule: {S3AutoDeletePolicy.ONE_DAY_LIFECYCLE_RULE_NAME} already in place for bucket: {bucketName}");
                return true;
            }
            return false;
        }

        private async Task<bool> CreateBucketPolicy(string bucketName, int days)
        {
            string id;
            string tagKey;
            string tagValue;
            if (days == 1)
            {
                id = S3AutoDeletePolicy.ONE_DAY_LIFECYCLE_RULE_NAME;
                tagKey = S3AutoDeletePolicy.ONE_DAY_EXPIRY_TAG_KEY;
                tagValue = S3AutoDeletePolicy.ONE_DAY_EXPIRY_TAG_VALUE;
            }
            else if(days == 2)
            {
                id = S3AutoDeletePolicy.TWO_DAY_LIFECYCLE_RULE_NAME;
                tagKey = S3AutoDeletePolicy.TWO_DAY_EXPIRY_TAG_KEY;
                tagValue = S3AutoDeletePolicy.TWO_DAY_EXPIRY_TAG_VALUE;
            }
            else if(days == 3)
            {
                id = S3AutoDeletePolicy.THREE_DAY_LIFECYCLE_RULE_NAME;
                tagKey = S3AutoDeletePolicy.THREE_DAY_EXPIRY_TAG_KEY;
                tagValue = S3AutoDeletePolicy.THREE_DAY_EXPIRY_TAG_VALUE;
            }
            else
            {
                id = "autodeletion" + Guid.NewGuid();
                tagKey = "file expires after days";
                tagValue = days.ToString();
            }
            var rule = new LifecycleRule
            {
                Id = id, // we may have separate rules for different days
                Filter = new LifecycleFilter()
                {
                    LifecycleFilterPredicate = new LifecycleAndOperator
                    {
                        Operands = new List<LifecycleFilterPredicate>
                    {
                        new LifecyclePrefixPredicate()
                        {
                            Prefix = string.Empty
                        },
                        new LifecycleTagPredicate()
                        {
                            Tag = new Tag
                            {
                                Key = tagKey,
                                Value =  tagValue
                            },
                        }
                    }
                    }
                },
                Status = LifecycleRuleStatus.Enabled,
                Expiration = new LifecycleRuleExpiration() { Days = days },
            };
            

            var bucketLifecycleConfiguration = new LifecycleConfiguration
            {
                Rules = new List<LifecycleRule>
                {
                    { rule }
                }
            };

            var putRequest = new PutLifecycleConfigurationRequest
            {
                BucketName = bucketName,
                Configuration = bucketLifecycleConfiguration
            };

            try
            {
                var data = await AmazonS3Client.PutLifecycleConfigurationAsync(putRequest);
                logger.LogInformation($"Bucket policy rule:{rule.Id} for bucket :{bucketName} created successfully.");
                return true;
            }
            catch (Exception ex)
            {
                logger.LogInformation($"Error creating bucket policy for bucket: {bucketName} ,{ex.InnerException}");
                return false;
            }
        }

        

        private async Task<bool> CreateBucketDataDeletePolicy(string bucketName, string path = "", int days = 1)
        {
            var stringManupulation = path.Split('/');
            var tenantName = stringManupulation[stringManupulation.Length - 1];
            var removeDataRule = new LifecycleRule
            {
                Id = string.Format(S3AutoDeletePolicy.CLEANUP_DATA_LIFECYCLE_RULE_NAME, bucketName, tenantName), // we may have separate rules for different days
                Filter = new LifecycleFilter()
                {
                    LifecycleFilterPredicate = new LifecyclePrefixPredicate()
                    {
                        Prefix = path
                    }
                },
                Status = LifecycleRuleStatus.Enabled,
                Expiration = new LifecycleRuleExpiration() { Days = days },
                NoncurrentVersionExpiration = new LifecycleRuleNoncurrentVersionExpiration() { NoncurrentDays = days },
                AbortIncompleteMultipartUpload = new LifecycleRuleAbortIncompleteMultipartUpload() { DaysAfterInitiation = days }
            };

            var removeDeleteMarkerRule = new LifecycleRule
            {
                Id = string.Format(S3AutoDeletePolicy.REMOVE_DELETEMARKER_LIFECYCLE_RULE_NAME, bucketName, tenantName), // we may have separate rules for different days
                Filter = new LifecycleFilter()
                {
                    LifecycleFilterPredicate = new LifecyclePrefixPredicate()
                    {
                        Prefix = path
                    }
                },
                Status = LifecycleRuleStatus.Enabled,
                Expiration = new LifecycleRuleExpiration() { ExpiredObjectDeleteMarker = true },
                AbortIncompleteMultipartUpload = new LifecycleRuleAbortIncompleteMultipartUpload() { DaysAfterInitiation = days, }
            };

            // Retreive existing lifecycle configuration and if(rules is empty){ create lifeycle rules } else { append the rules in lifeycle configuration } 
            LifecycleConfiguration bucketLifecycleConfiguration;
            var configurationRequest = new GetLifecycleConfigurationRequest
            {
                BucketName = bucketName
            };

            var configurationResponse = await this.AmazonS3Client.GetLifecycleConfigurationAsync(configurationRequest);
            if(configurationResponse.Configuration != null)
            {
                bucketLifecycleConfiguration = configurationResponse.Configuration;
                bucketLifecycleConfiguration.Rules.Add(removeDataRule);
                bucketLifecycleConfiguration.Rules.Add(removeDeleteMarkerRule);
                var putRequest = new PutLifecycleConfigurationRequest
                {
                    BucketName = bucketName,
                    Configuration = bucketLifecycleConfiguration
                };
                try
                {
                    var data = await AmazonS3Client.PutLifecycleConfigurationAsync(putRequest);
                    logger.LogInformation($"Bucket policy rule:{removeDataRule.Id} for bucket :{bucketName} created successfully.");
                    logger.LogInformation($"Bucket policy rule:{removeDeleteMarkerRule.Id} for bucket :{bucketName} created successfully.");
                    return true;
                }
                catch (Exception ex)
                {
                    logger.LogError($"Error creating bucket policy for bucket: {bucketName} ,{ex.InnerException}");
                    return false;
                }
            }
            else
            {
                bucketLifecycleConfiguration = new LifecycleConfiguration
                {
                    Rules = new List<LifecycleRule> { removeDataRule, removeDeleteMarkerRule }
                };

                var putRequest = new PutLifecycleConfigurationRequest
                {
                    BucketName = bucketName,
                    Configuration = bucketLifecycleConfiguration
                };

                try
                {
                    var data = await AmazonS3Client.PutLifecycleConfigurationAsync(putRequest);
                    logger.LogError($"Bucket policy rule:{removeDataRule.Id} for bucket :{bucketName} created successfully.");
                    logger.LogError($"Bucket policy rule:{removeDeleteMarkerRule.Id} for bucket :{bucketName} created successfully.");
                    return true;
                }
                catch (Exception ex)
                {
                    logger.LogError($"Error creating bucket policy for bucket: {bucketName} ,{ex.InnerException}");
                    return false;
                }
            }
        }
    }

    public sealed class S3AutoDeletePolicy
    {
        private S3AutoDeletePolicy() { }

        public const string POLICY_BUCKET_PREFIX = "filestore/";
        public const string ONE_DAY_LIFECYCLE_RULE_NAME = "FileOneDayExpiryRule";
        public const string TWO_DAY_LIFECYCLE_RULE_NAME = "FileTwoDayExpiryRule";
        public const string THREE_DAY_LIFECYCLE_RULE_NAME = "FileThreeDayExpiryRule";
        public const string ONE_DAY_EXPIRY_TAG_KEY = "FileExpiresAfterDays";
        public const string ONE_DAY_EXPIRY_TAG_VALUE = "1";
        public const string TWO_DAY_EXPIRY_TAG_KEY = "FileExpiresAfterDays";
        public const string THREE_DAY_EXPIRY_TAG_KEY = "FileExpiresAfterDays";
        public const string TWO_DAY_EXPIRY_TAG_VALUE = "2";
        public const string THREE_DAY_EXPIRY_TAG_VALUE = "3";
        public const string CLEANUP_DATA_LIFECYCLE_RULE_NAME = "Cleanup_{0}_{1}";
        public const string REMOVE_DELETEMARKER_LIFECYCLE_RULE_NAME = "RmMarker_{0}_{1}";
    }
}
